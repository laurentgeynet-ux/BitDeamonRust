//! Owned bencode value — port of `libtorrent::entry` (`entry.hpp`/`entry.cpp`).
//!
//! `Entry` is the mutable, owning counterpart to the zero-copy
//! [`BdecodeNode`](crate::BdecodeNode). Dict keys are raw bytes (`Vec<u8>`)
//! stored in a `BTreeMap`, matching upstream's `map<std::string, entry,
//! strview_less>`: sorted, deduplicated, byte-wise lexicographic order.

use std::collections::BTreeMap;
use std::fmt;

use crate::node::{BdecodeNode, NodeType};

/// `entry::dictionary_type` — sorted byte-keyed map.
pub type Dictionary = BTreeMap<Vec<u8>, Entry>;
/// `entry::list_type`.
pub type List = Vec<Entry>;
/// `entry::string_type` — raw bytes, not necessarily UTF-8.
pub type ByteString = Vec<u8>;
/// `entry::integer_type`.
pub type Integer = i64;
/// `entry::preformatted_type` — verbatim re-encoded bytes.
pub type Preformatted = Vec<u8>;

/// `entry::data_type` — the concrete type tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryType {
    /// `int_t`.
    Int,
    /// `string_t`.
    String,
    /// `list_t`.
    List,
    /// `dictionary_t`.
    Dict,
    /// `preformatted_t` — raw bytes re-encoded verbatim.
    Preformatted,
    /// `undefined_t` — default-constructed / uninitialized.
    Undefined,
}

/// Type-mismatch on an accessor — upstream throws `system_error(type_error)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("entry type mismatch: not a {expected}")]
pub struct EntryTypeError {
    /// What the accessor required.
    pub expected: &'static str,
}

/// `libtorrent::entry`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Entry {
    /// `uninitialized_type` — default construction.
    #[default]
    Undefined,
    /// `integer_type`.
    Int(Integer),
    /// `string_type` — raw bytes.
    String(ByteString),
    /// `list_type`.
    List(List),
    /// `dictionary_type`.
    Dict(Dictionary),
    /// `preformatted_type` — bencoded bytes emitted verbatim on encode.
    Preformatted(Preformatted),
}

impl Entry {
    /// `entry::type()`.
    #[must_use]
    pub fn entry_type(&self) -> EntryType {
        match self {
            Self::Int(_) => EntryType::Int,
            Self::String(_) => EntryType::String,
            Self::List(_) => EntryType::List,
            Self::Dict(_) => EntryType::Dict,
            Self::Preformatted(_) => EntryType::Preformatted,
            Self::Undefined => EntryType::Undefined,
        }
    }

    /// `entry(bdecode_node)` — deep-copy a decoded tree into an owned value.
    /// A `none` node maps to `Undefined`.
    #[must_use]
    pub fn from_node(node: &BdecodeNode<'_>) -> Self {
        match node.node_type() {
            NodeType::Int => Self::Int(node.int_value()),
            NodeType::String => Self::String(node.string_value().to_vec()),
            NodeType::List => {
                let mut l = Vec::with_capacity(node.list_size());
                for i in 0..node.list_size() {
                    l.push(Self::from_node(&node.list_at(i)));
                }
                Self::List(l)
            }
            NodeType::Dict => {
                let mut d = Dictionary::new();
                for i in 0..node.dict_size() {
                    let (key, value) = node.dict_at(i);
                    d.insert(key.to_vec(), Self::from_node(&value));
                }
                Self::Dict(d)
            }
            NodeType::None => Self::Undefined,
        }
    }

    /// `integer()` — the `i64`, or `Err` on type mismatch.
    pub fn integer(&self) -> Result<Integer, EntryTypeError> {
        match self {
            Self::Int(v) => Ok(*v),
            _ => Err(EntryTypeError {
                expected: "integer",
            }),
        }
    }

    /// Mutable `integer()`.
    pub fn integer_mut(&mut self) -> Result<&mut Integer, EntryTypeError> {
        match self {
            Self::Int(v) => Ok(v),
            _ => Err(EntryTypeError {
                expected: "integer",
            }),
        }
    }

    /// `string()` — the raw bytes.
    pub fn string(&self) -> Result<&[u8], EntryTypeError> {
        match self {
            Self::String(s) => Ok(s),
            _ => Err(EntryTypeError { expected: "string" }),
        }
    }

    /// Mutable `string()`.
    pub fn string_mut(&mut self) -> Result<&mut ByteString, EntryTypeError> {
        match self {
            Self::String(s) => Ok(s),
            _ => Err(EntryTypeError { expected: "string" }),
        }
    }

    /// `list()`.
    pub fn list(&self) -> Result<&List, EntryTypeError> {
        match self {
            Self::List(l) => Ok(l),
            _ => Err(EntryTypeError { expected: "list" }),
        }
    }

    /// Mutable `list()`.
    pub fn list_mut(&mut self) -> Result<&mut List, EntryTypeError> {
        match self {
            Self::List(l) => Ok(l),
            _ => Err(EntryTypeError { expected: "list" }),
        }
    }

    /// `dict()`.
    pub fn dict(&self) -> Result<&Dictionary, EntryTypeError> {
        match self {
            Self::Dict(d) => Ok(d),
            _ => Err(EntryTypeError {
                expected: "dictionary",
            }),
        }
    }

    /// Mutable `dict()`.
    pub fn dict_mut(&mut self) -> Result<&mut Dictionary, EntryTypeError> {
        match self {
            Self::Dict(d) => Ok(d),
            _ => Err(EntryTypeError {
                expected: "dictionary",
            }),
        }
    }

    /// `preformatted()`.
    pub fn preformatted(&self) -> Result<&Preformatted, EntryTypeError> {
        match self {
            Self::Preformatted(p) => Ok(p),
            _ => Err(EntryTypeError {
                expected: "preformatted",
            }),
        }
    }

    /// Mutable `preformatted()`.
    pub fn preformatted_mut(&mut self) -> Result<&mut Preformatted, EntryTypeError> {
        match self {
            Self::Preformatted(p) => Ok(p),
            _ => Err(EntryTypeError {
                expected: "preformatted",
            }),
        }
    }

    /// `find_key(key)` — look up `key` in a dict entry; `None` if absent.
    /// Requires the entry to be a dict (upstream throws otherwise).
    pub fn find_key(&self, key: &[u8]) -> Option<&Entry> {
        match self {
            Self::Dict(d) => d.get(key),
            _ => None,
        }
    }

    /// Mutable `find_key`.
    pub fn find_key_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        match self {
            Self::Dict(d) => d.get_mut(key),
            _ => None,
        }
    }

    /// `operator[](key)` non-const — existing value or a fresh `Undefined`.
    /// Requires the entry to be a dict (upstream asserts via `dict()`).
    ///
    /// # Panics
    /// If `self` is not a `Dict`.
    pub fn entry_mut(&mut self, key: &[u8]) -> &mut Entry {
        match self {
            Self::Dict(d) => d.entry(key.to_vec()).or_insert(Entry::Undefined),
            _ => panic!("entry_mut on non-dict entry"),
        }
    }

    /// `to_string()` — JSON-ish pretty print (same shape as `print_entry`).
    #[must_use]
    pub fn to_string_pretty(&self, single_line: bool) -> String {
        crate::print::print_entry(self, single_line, 0)
    }
}

impl From<Integer> for Entry {
    fn from(v: Integer) -> Self {
        Self::Int(v)
    }
}
impl From<ByteString> for Entry {
    fn from(v: ByteString) -> Self {
        Self::String(v)
    }
}
impl From<&[u8]> for Entry {
    fn from(v: &[u8]) -> Self {
        Self::String(v.to_vec())
    }
}
impl From<&str> for Entry {
    fn from(v: &str) -> Self {
        Self::String(v.as_bytes().to_vec())
    }
}
impl From<List> for Entry {
    fn from(v: List) -> Self {
        Self::List(v)
    }
}
impl From<Dictionary> for Entry {
    fn from(v: Dictionary) -> Self {
        Self::Dict(v)
    }
}
// NOTE: no `From<Preformatted>` — `Preformatted` and `ByteString` are both
// `Vec<u8>`; construct `Entry::Preformatted(bytes)` explicitly.
impl From<&BdecodeNode<'_>> for Entry {
    fn from(v: &BdecodeNode<'_>) -> Self {
        Self::from_node(v)
    }
}

impl fmt::Display for Entry {
    /// `entry::to_string()` default (multi-line, `single_line = false`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_string_pretty(false))
    }
}
