//! Port of `bdecode.cpp`/`bdecode.hpp` (`bdecode_node`).
//!
//! `BdecodeNode` — port of `libtorrent::bdecode_node`.
//!
//! A node is a lightweight view into a flat token stream plus the source
//! buffer. The token vec is shared (`Rc<[BdecodeToken]>`) so child nodes are
//! cheap to produce and outlive their parent — a safe equivalent of
//! upstream's `m_root_tokens` raw pointer + `non_owning()` split. This is
//! slightly *stronger* than upstream, where children dangle if the owning
//! root dies.

use std::cell::Cell;
use std::rc::Rc;

use crate::error::BdecodeError;
use crate::token::{BdecodeToken, TokenKind};

/// `bdecode_node::type_t` — the public node kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    /// `none_t` — uninitialized/absent node (e.g. `dict_find` miss).
    None,
    /// `int_t`.
    Int,
    /// `string_t` — covers upstream `string` and `long_string` tokens.
    String,
    /// `list_t`.
    List,
    /// `dict_t`.
    Dict,
}

/// Soft-error categories detected by [`BdecodeNode::has_soft_error`]
/// (upstream writes a message into a caller buffer; we return the kind).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SoftError {
    /// `i03e`-style leading zero on an integer.
    LeadingZeroInt,
    /// `03:x`-style leading zero on a string length.
    LeadingZeroStringLength,
    /// Dictionary keys not in canonical sorted order.
    UnsortedDictKey,
    /// The same key appears twice in one dictionary.
    DuplicateDictKey,
}

impl SoftError {
    /// The exact strings upstream writes into the caller's buffer.
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            Self::LeadingZeroInt => "leading zero in integer",
            Self::LeadingZeroStringLength => "leading zero in string length",
            Self::UnsortedDictKey => "unsorted dictionary key",
            Self::DuplicateDictKey => "duplicate dictionary key",
        }
    }
}

/// `libtorrent::bdecode_node`.
///
/// Obtained from [`crate::bdecode`]. All string accessors return slices of the
/// original input — zero-copy.
#[derive(Clone, Debug)]
pub struct BdecodeNode<'buf> {
    /// Shared token stream; empty for a `none` node.
    tokens: Rc<[BdecodeToken]>,
    /// The bencoded buffer this was parsed from.
    buffer: &'buf [u8],
    /// `m_buffer_size` — bytes consumed by the parse (≤ `buffer.len()`).
    buffer_len: usize,
    /// `m_token_idx` — `-1` for a `none` node.
    token_idx: i64,
    /// `m_last_index` — dict/list lookup cursor cache.
    last_index: Cell<i64>,
    /// `m_last_token`.
    last_token: Cell<i64>,
    /// `m_size` — cached `dict_size`/`list_size`, `-1` unknown.
    size: Cell<i64>,
    /// Parse error, set only on the root returned by `bdecode`.
    error: Option<BdecodeError>,
}

impl<'buf> BdecodeNode<'buf> {
    /// A `none` node (`bdecode_node()` default / cleared node).
    #[must_use]
    pub fn none() -> Self {
        Self {
            tokens: Rc::from(Vec::new()),
            buffer: b"",
            buffer_len: 0,
            token_idx: -1,
            last_index: Cell::new(-1),
            last_token: Cell::new(-1),
            size: Cell::new(-1),
            error: None,
        }
    }

    /// Root constructor used by the parser.
    pub(crate) fn new(
        tokens: Vec<BdecodeToken>,
        buffer: &'buf [u8],
        consumed: usize,
        error: Option<BdecodeError>,
    ) -> Self {
        Self {
            tokens: Rc::from(tokens),
            buffer,
            buffer_len: consumed,
            token_idx: 0,
            last_index: Cell::new(-1),
            last_token: Cell::new(-1),
            size: Cell::new(-1),
            error,
        }
    }

    /// Node for the `> max_offset` early failure — a `none` node with error.
    pub(crate) fn failed(buffer: &'buf [u8], error: BdecodeError) -> Self {
        Self {
            tokens: Rc::from(Vec::new()),
            buffer,
            buffer_len: 0,
            token_idx: -1,
            last_index: Cell::new(-1),
            last_token: Cell::new(-1),
            size: Cell::new(-1),
            error: Some(error),
        }
    }

    /// Child-node constructor (`bdecode_node(tokens, buf, len, idx)`).
    fn at(&self, idx: usize) -> Self {
        Self {
            tokens: Rc::clone(&self.tokens),
            buffer: self.buffer,
            buffer_len: self.buffer_len,
            token_idx: idx as i64,
            last_index: Cell::new(-1),
            last_token: Cell::new(-1),
            size: Cell::new(-1),
            error: None,
        }
    }

    /// The parse error — only ever `Some` on the root returned by `bdecode`.
    /// `None` on success and on all child nodes.
    #[must_use]
    pub fn error(&self) -> Option<BdecodeError> {
        self.error
    }

    /// `operator bool` / `type() != none_t` — does this node hold a value?
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.token_idx != -1
    }

    /// `clear()` — reset to a `none` node.
    pub fn clear(&mut self) {
        *self = Self::none();
    }

    /// `switch_underlying_buffer` — repoint at a different buffer holding the
    /// same bencoded layout (upstream asserts this; callers must uphold it).
    /// Children keep their own `Rc` of the tokens, so they stay valid.
    pub fn switch_underlying_buffer(&mut self, buffer: &'buf [u8]) {
        if self.tokens.is_empty() {
            return;
        }
        self.buffer = buffer;
    }

    /// `type()` — the node's concrete type.
    #[must_use]
    pub fn node_type(&self) -> NodeType {
        if self.token_idx == -1 {
            return NodeType::None;
        }
        match self.token().kind() {
            TokenKind::None | TokenKind::End => NodeType::None,
            TokenKind::Integer => NodeType::Int,
            TokenKind::String | TokenKind::LongString => NodeType::String,
            TokenKind::List => NodeType::List,
            TokenKind::Dict => NodeType::Dict,
        }
    }

    /// The current token (caller guarantees `token_idx != -1`).
    fn token(&self) -> BdecodeToken {
        self.tokens[self.token_idx as usize]
    }

    /// `token_source_span` — full source length of a token, including header
    /// and terminator (`i…e`, `NNN:…`, `d…e`). Only valid for non-last tokens;
    /// the physical next token's offset is its own source start.
    fn token_source_span(&self, t: BdecodeToken) -> usize {
        // caller must only invoke on a token that has a physical successor
        self.tokens[self.token_idx as usize + t.next_item() as usize].offset() as usize
            - t.offset() as usize
    }

    /// `data_section()` — the node's exact source bytes, for info-hash hashing.
    /// Empty slice on a `none` node.
    #[must_use]
    pub fn data_section(&self) -> &'buf [u8] {
        if self.token_idx == -1 {
            return &self.buffer[..0];
        }
        let t = self.token();
        let start = t.offset() as usize;
        let next = self.tokens[self.token_idx as usize + t.next_item() as usize];
        let end = next.offset() as usize;
        if end < start || next.kind() == TokenKind::None {
            return &self.buffer[..0];
        }
        &self.buffer[start..end]
    }

    /// `data_offset()` — byte offset where this node's source starts.
    /// `None` (well, `Option::None`) on a `none` node (upstream returns -1).
    #[must_use]
    pub fn data_offset(&self) -> Option<usize> {
        if self.token_idx == -1 {
            return None;
        }
        Some(self.token().offset() as usize)
    }

    /// `int_value()` — parses `i…e` lazily from the buffer, like upstream.
    /// Returns 0 if the node isn't an int or the value overflows.
    #[must_use]
    pub fn int_value(&self) -> i64 {
        if self.node_type() != NodeType::Int {
            return 0;
        }
        let t = self.token();
        let start = t.offset() as usize + 1; // skip 'i'
        let mut val: i64 = 0;
        let negative = self.buffer[start] == b'-';
        // upstream ignores the error here and returns 0 on parse failure
        if crate::decode::parse_int(self.buffer, start + usize::from(negative), b'e', &mut val)
            .is_err()
        {
            return 0;
        }
        if negative {
            val = -val;
        }
        val
    }

    /// `string_value()` — the raw bytes (bencode strings are binary-safe).
    /// Empty slice if the node isn't a string.
    #[must_use]
    pub fn string_value(&self) -> &'buf [u8] {
        if self.node_type() != NodeType::String {
            return &self.buffer[..0];
        }
        let t = self.token();
        let start = t.offset() as usize + t.start_offset() as usize;
        let len = self.token_source_span(t) - t.start_offset() as usize;
        &self.buffer[start..start + len]
    }

    /// `string_length()`.
    #[must_use]
    pub fn string_length(&self) -> usize {
        if self.node_type() != NodeType::String {
            return 0;
        }
        let t = self.token();
        self.token_source_span(t) - t.start_offset() as usize
    }

    /// `string_offset()` — byte offset of the string's *value* in the buffer.
    #[must_use]
    pub fn string_offset(&self) -> Option<usize> {
        if self.node_type() != NodeType::String {
            return None;
        }
        let t = self.token();
        Some(t.offset() as usize + t.start_offset() as usize)
    }

    /// `list_at(i)` — the `i`-th element of a list node.
    ///
    /// Panics on a non-list node (upstream asserts). Out-of-range `i` hits the
    /// list's `end` token and returns a `none` node — same observable result
    /// as upstream's assert-disabled behavior, without panicking on wire data.
    #[must_use]
    pub fn list_at(&self, i: usize) -> BdecodeNode<'buf> {
        assert_eq!(self.node_type(), NodeType::List);
        let mut token = self.token_idx as usize + 1;
        let mut item = 0usize;

        // resume from the cached cursor when possible
        if self.last_index.get() != -1 && self.last_index.get() <= i as i64 {
            token = self.last_token.get() as usize;
            item = self.last_index.get() as usize;
        }

        while item < i {
            token += self.tokens[token].next_item() as usize;
            item += 1;
            if self.tokens[token].kind() == TokenKind::End {
                return BdecodeNode::none();
            }
        }

        self.last_token.set(token as i64);
        self.last_index.set(i as i64);
        self.at(token)
    }

    /// `list_size()`.
    #[must_use]
    pub fn list_size(&self) -> usize {
        assert_eq!(self.node_type(), NodeType::List);
        if self.size.get() != -1 {
            return self.size.get() as usize;
        }

        let mut token = self.token_idx as usize + 1;
        let mut ret = 0usize;

        if self.last_index.get() != -1 {
            token = self.last_token.get() as usize;
            ret = self.last_index.get() as usize;
        }
        while self.tokens[token].kind() != TokenKind::End {
            token += self.tokens[token].next_item() as usize;
            ret += 1;
        }

        self.size.set(ret as i64);
        ret
    }

    /// `list_string_value_at(i, default)`.
    #[must_use]
    pub fn list_string_value_at<'a>(&self, i: usize, default: &'a [u8]) -> &'a [u8]
    where
        'buf: 'a,
    {
        let n = self.list_at(i);
        if n.node_type() != NodeType::String {
            return default;
        }
        n.string_value()
    }

    /// `list_int_value_at(i, default)`.
    #[must_use]
    pub fn list_int_value_at(&self, i: usize, default: i64) -> i64 {
        let n = self.list_at(i);
        if n.node_type() != NodeType::Int {
            return default;
        }
        n.int_value()
    }

    /// `dict_at_node(i)` — `(key_node, value_node)` pair `i`.
    #[must_use]
    pub fn dict_at_node(&self, i: usize) -> (BdecodeNode<'buf>, BdecodeNode<'buf>) {
        assert_eq!(self.node_type(), NodeType::Dict);
        let mut token = self.token_idx as usize + 1;
        let mut item = 0usize;

        if self.last_index.get() != -1 && self.last_index.get() <= i as i64 {
            token = self.last_token.get() as usize;
            item = self.last_index.get() as usize;
        }

        while item < i {
            // skip key
            token += self.tokens[token].next_item() as usize;
            if self.tokens[token].kind() == TokenKind::End {
                return (BdecodeNode::none(), BdecodeNode::none());
            }
            // skip value
            token += self.tokens[token].next_item() as usize;
            item += 1;
            if self.tokens[token].kind() == TokenKind::End {
                return (BdecodeNode::none(), BdecodeNode::none());
            }
        }

        // upstream doesn't cache the first item
        if i > 0 {
            self.last_token.set(token as i64);
            self.last_index.set(i as i64);
        }

        let value_token = token + self.tokens[token].next_item() as usize;
        if self.tokens[token].kind() == TokenKind::End {
            return (BdecodeNode::none(), BdecodeNode::none());
        }
        (self.at(token), self.at(value_token))
    }

    /// `dict_at(i)` — `(key_bytes, value_node)` pair `i`.
    #[must_use]
    pub fn dict_at(&self, i: usize) -> (&'buf [u8], BdecodeNode<'buf>) {
        let (key, value) = self.dict_at_node(i);
        (key.string_value(), value)
    }

    /// `dict_size()` — number of key/value pairs.
    #[must_use]
    pub fn dict_size(&self) -> usize {
        assert_eq!(self.node_type(), NodeType::Dict);
        if self.size.get() != -1 {
            return self.size.get() as usize;
        }

        let mut token = self.token_idx as usize + 1;
        let mut ret = 0usize;

        if self.last_index.get() != -1 {
            ret = self.last_index.get() as usize * 2;
            token = self.last_token.get() as usize;
        }
        while self.tokens[token].kind() != TokenKind::End {
            token += self.tokens[token].next_item() as usize;
            ret += 1;
        }
        debug_assert_eq!(ret % 2, 0);
        ret /= 2;

        self.size.set(ret as i64);
        ret
    }

    /// `dict_find(key)` — value node for `key`, or a `none` node.
    /// Plain linear scan over key tokens, exactly like upstream.
    #[must_use]
    pub fn dict_find(&self, key: &[u8]) -> BdecodeNode<'buf> {
        if self.node_type() != NodeType::Dict {
            return BdecodeNode::none();
        }
        let mut token = self.token_idx as usize + 1;
        while self.tokens[token].kind() != TokenKind::End {
            let t = self.tokens[token];
            debug_assert!(matches!(
                t.kind(),
                TokenKind::String | TokenKind::LongString
            ));
            let size = self.token_source_span_at(token) - t.start_offset() as usize;
            let kstart = t.offset() as usize + t.start_offset() as usize;
            if key.len() == size && self.buffer[kstart..kstart + size] == *key {
                // skip key
                token += t.next_item() as usize;
                if self.tokens[token].kind() == TokenKind::End {
                    return BdecodeNode::none();
                }
                return self.at(token);
            }
            // skip key
            token += t.next_item() as usize;
            if self.tokens[token].kind() == TokenKind::End {
                return BdecodeNode::none();
            }
            // skip value
            token += self.tokens[token].next_item() as usize;
        }
        BdecodeNode::none()
    }

    /// `token_source_span` for an arbitrary token index (used by dict_find,
    /// where `self.token_idx` isn't the scanned token).
    fn token_source_span_at(&self, token_idx: usize) -> usize {
        self.tokens[token_idx + 1].offset() as usize - self.tokens[token_idx].offset() as usize
    }

    /// `dict_find_*` typed accessors — `none` node on type mismatch.
    #[must_use]
    pub fn dict_find_list(&self, key: &[u8]) -> BdecodeNode<'buf> {
        let ret = self.dict_find(key);
        if ret.node_type() == NodeType::List {
            return ret;
        }
        BdecodeNode::none()
    }

    /// See [`Self::dict_find_list`].
    #[must_use]
    pub fn dict_find_dict(&self, key: &[u8]) -> BdecodeNode<'buf> {
        let ret = self.dict_find(key);
        if ret.node_type() == NodeType::Dict {
            return ret;
        }
        BdecodeNode::none()
    }

    /// See [`Self::dict_find_list`].
    #[must_use]
    pub fn dict_find_string(&self, key: &[u8]) -> BdecodeNode<'buf> {
        let ret = self.dict_find(key);
        if ret.node_type() == NodeType::String {
            return ret;
        }
        BdecodeNode::none()
    }

    /// See [`Self::dict_find_list`].
    #[must_use]
    pub fn dict_find_int(&self, key: &[u8]) -> BdecodeNode<'buf> {
        let ret = self.dict_find(key);
        if ret.node_type() == NodeType::Int {
            return ret;
        }
        BdecodeNode::none()
    }

    /// `dict_find_string_value(key, default)`.
    #[must_use]
    pub fn dict_find_string_value<'a>(&self, key: &[u8], default: &'a [u8]) -> &'a [u8]
    where
        'buf: 'a,
    {
        let n = self.dict_find(key);
        if n.node_type() != NodeType::String {
            return default;
        }
        n.string_value()
    }

    /// `dict_find_int_value(key, default)`.
    #[must_use]
    pub fn dict_find_int_value(&self, key: &[u8], default: i64) -> i64 {
        let n = self.dict_find(key);
        if n.node_type() != NodeType::Int {
            return default;
        }
        n.int_value()
    }

    /// `has_soft_error` — detect non-fatal spec violations (unsorted or
    /// duplicate dict keys, leading zeros). Returns the first found.
    ///
    /// Upstream writes a message into a caller-provided buffer; the returned
    /// [`SoftError`] maps 1:1 to those strings.
    #[must_use]
    pub fn has_soft_error(&self) -> Option<SoftError> {
        if self.node_type() == NodeType::None {
            return None;
        }

        let tokens = &*self.tokens;
        let mut stack: Vec<usize> = Vec::with_capacity(100);
        let mut token = self.token_idx as usize;

        loop {
            match tokens[token].kind() {
                TokenKind::Integer => {
                    let off = tokens[token].offset() as usize;
                    if self.buffer[off + 1] == b'0' && self.buffer[off + 2] != b'e' {
                        return Some(SoftError::LeadingZeroInt);
                    }
                }
                TokenKind::String | TokenKind::LongString => {
                    let off = tokens[token].offset() as usize;
                    if self.buffer[off] == b'0' && self.buffer[off + 1] != b':' {
                        return Some(SoftError::LeadingZeroStringLength);
                    }
                }
                TokenKind::Dict | TokenKind::List => stack.push(token),
                TokenKind::End | TokenKind::None => {
                    let parent = stack.pop()?;
                    if tokens[parent].kind() == TokenKind::Dict && token != parent + 1 {
                        // end of a non-empty dict — verify key order
                        let mut k1 = parent + 1;
                        loop {
                            // skip to the first key's value, then to the next key
                            let v1 = k1 + tokens[k1].next_item() as usize;
                            let k2 = v1 + tokens[v1].next_item() as usize;
                            if k2 == token {
                                break;
                            }
                            let v2 = k2 + tokens[k2].next_item() as usize;

                            let k1_start =
                                tokens[k1].offset() as usize + tokens[k1].start_offset() as usize;
                            let k1_len = tokens[v1].offset() as usize - k1_start;
                            let k2_start =
                                tokens[k2].offset() as usize + tokens[k2].start_offset() as usize;
                            let k2_len = tokens[v2].offset() as usize - k2_start;

                            let min_len = k1_len.min(k2_len);
                            let cmp = self.buffer[k1_start..k1_start + min_len]
                                .cmp(&self.buffer[k2_start..k2_start + min_len]);
                            match cmp {
                                std::cmp::Ordering::Greater => {
                                    return Some(SoftError::UnsortedDictKey);
                                }
                                std::cmp::Ordering::Equal if k1_len > k2_len => {
                                    return Some(SoftError::UnsortedDictKey);
                                }
                                std::cmp::Ordering::Equal if k1_len == k2_len => {
                                    return Some(SoftError::DuplicateDictKey);
                                }
                                _ => {}
                            }
                            k1 = k2;
                        }
                    }
                }
            }

            token += 1;
            if stack.is_empty() {
                return None;
            }
        }
    }
}
