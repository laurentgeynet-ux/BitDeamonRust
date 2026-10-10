//! Packed bdecode token — port of `aux::bdecode_token`.
//!
//! Upstream packs each token into 8 bytes: `offset:29 | type:3` in the first
//! word, `next_item:29 | header:3` in the second. We keep the exact same
//! packing so the limit constants (`max_offset`, `max_next_item`,
//! `short_string_max_header`, `long_string_max_header`) carry over verbatim
//! and token storage stays at 8 bytes.

/// Token type — discriminants match `bdecode_token::type_t`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum TokenKind {
    /// `bdecode_token::none` — empty/default token.
    None = 0,
    /// `bdecode_token::dict`.
    Dict = 1,
    /// `bdecode_token::list`.
    List = 2,
    /// `bdecode_token::string` — header ≤ `SHORT_STRING_MAX_HEADER`.
    String = 3,
    /// `bdecode_token::integer`.
    Integer = 4,
    /// `bdecode_token::long_string` — length prefix needs >8 digits.
    LongString = 5,
    /// `bdecode_token::end` — logical end-of-container token.
    End = 6,
}

/// `bdecode_token::limits_t`.
pub(crate) const MAX_OFFSET: u32 = (1 << 29) - 1;
pub(crate) const MAX_NEXT_ITEM: u32 = (1 << 29) - 1;
pub(crate) const SHORT_STRING_MAX_HEADER: u32 = (1 << 3) - 1 + 2; // 9
pub(crate) const LONG_STRING_MAX_HEADER: u32 = 8 + (1 << 3) - 1 + 2; // 17

/// `aux::bdecode_token` — packed into two `u32` words.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BdecodeToken {
    /// `offset:29 | kind:3`
    w0: u32,
    /// `next_item:29 | header:3`
    w1: u32,
}

impl BdecodeToken {
    /// `bdecode_token(off, type)` — scalar/container token without header.
    pub(crate) fn new(offset: u32, kind: TokenKind) -> Self {
        debug_assert!(offset <= MAX_OFFSET);
        Self {
            w0: ((kind as u32) << 29) | offset,
            w1: 0,
        }
    }

    /// `bdecode_token(off, next, type, header_size)` — with next-item pointer
    /// and string header. `header_size` is the full `"NNN:"` width (digits +
    /// colon); headers > `SHORT_STRING_MAX_HEADER` promote `String` to
    /// `LongString` and store `header_size - 10`.
    pub(crate) fn with_next(
        offset: u32,
        next_item: u32,
        kind: TokenKind,
        header_size: u32,
    ) -> Self {
        debug_assert!(offset <= MAX_OFFSET);
        debug_assert!(next_item <= MAX_NEXT_ITEM);
        let (kind, header) = match kind {
            TokenKind::String if header_size > SHORT_STRING_MAX_HEADER => {
                (TokenKind::LongString, header_size - 8 - 2)
            }
            TokenKind::String => (TokenKind::String, header_size - 2),
            _ => (kind, 0),
        };
        debug_assert!(header <= 7);
        Self {
            w0: ((kind as u32) << 29) | offset,
            w1: (header << 29) | next_item,
        }
    }

    /// `bdecode_token::offset` — byte offset of the token's source start.
    #[must_use]
    pub(crate) fn offset(self) -> u32 {
        self.w0 & MAX_OFFSET
    }

    /// `bdecode_token::type`.
    #[must_use]
    pub(crate) fn kind(self) -> TokenKind {
        match self.w0 >> 29 {
            0 => TokenKind::None,
            1 => TokenKind::Dict,
            2 => TokenKind::List,
            3 => TokenKind::String,
            4 => TokenKind::Integer,
            5 => TokenKind::LongString,
            _ => TokenKind::End,
        }
    }

    /// `bdecode_token::next_item` — relative index to the next sibling token.
    #[must_use]
    pub(crate) fn next_item(self) -> u32 {
        self.w1 & MAX_NEXT_ITEM
    }

    pub(crate) fn set_next_item(&mut self, next: u32) {
        debug_assert!(next <= MAX_NEXT_ITEM);
        self.w1 = (self.w1 & !MAX_NEXT_ITEM) | next;
    }

    /// `bdecode_token::start_offset()` — for strings, the `"NNN:"` header
    /// width; 0 for other kinds.
    #[must_use]
    pub(crate) fn start_offset(self) -> u32 {
        match self.kind() {
            TokenKind::String => (self.w1 >> 29) + 2,
            TokenKind::LongString => (self.w1 >> 29) + 8 + 2,
            _ => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packing_roundtrip() {
        let t = BdecodeToken::with_next(12345, 42, TokenKind::Dict, 0);
        assert_eq!(t.offset(), 12345);
        assert_eq!(t.next_item(), 42);
        assert_eq!(t.kind(), TokenKind::Dict);
        assert_eq!(t.start_offset(), 0);

        // short string "3:aaa" → header 2
        let t = BdecodeToken::with_next(10, 1, TokenKind::String, 2);
        assert_eq!(t.kind(), TokenKind::String);
        assert_eq!(t.start_offset(), 2);

        // 9-char header stays "string"
        let t = BdecodeToken::with_next(0, 1, TokenKind::String, 9);
        assert_eq!(t.kind(), TokenKind::String);
        assert_eq!(t.start_offset(), 9);

        // 10+ char header promotes to long_string
        let t = BdecodeToken::with_next(0, 1, TokenKind::String, 10);
        assert_eq!(t.kind(), TokenKind::LongString);
        assert_eq!(t.start_offset(), 10);

        // max long header
        let t = BdecodeToken::with_next(0, 1, TokenKind::String, 17);
        assert_eq!(t.kind(), TokenKind::LongString);
        assert_eq!(t.start_offset(), 17);
    }
}
