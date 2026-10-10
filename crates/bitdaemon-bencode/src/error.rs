//! Decode error kinds — port of `bdecode_errors::error_code_enum` and the
//! `bdecode_error_category::message()` strings.

/// `bdecode_errors::error_code_enum` — one variant per upstream code,
/// same order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BdecodeErrorKind {
    /// `expected_digit` — expected digit in bencoded string.
    ExpectedDigit,
    /// `expected_colon` — expected colon in bencoded string.
    ExpectedColon,
    /// `unexpected_eof` — unexpected end of file in bencoded string.
    UnexpectedEof,
    /// `expected_value` — expected value (list, dict, int or string).
    ExpectedValue,
    /// `depth_exceeded` — bencoded nesting depth limit exceeded.
    DepthExceeded,
    /// `limit_exceeded` — bencoded item count limit exceeded.
    LimitExceeded,
    /// `overflow` — integer overflow.
    Overflow,
}

impl BdecodeErrorKind {
    /// `bdecode_error_category::message()` — the exact upstream strings.
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            Self::ExpectedDigit => "expected digit in bencoded string",
            Self::ExpectedColon => "expected colon in bencoded string",
            Self::UnexpectedEof => "unexpected end of file in bencoded string",
            Self::ExpectedValue => "expected value (list, dict, int or string) in bencoded string",
            Self::DepthExceeded => "bencoded nesting depth exceeded",
            Self::LimitExceeded => "bencoded item count limit exceeded",
            Self::Overflow => "integer overflow",
        }
    }
}

/// A decode failure: `error_code` + `error_pos` out-params from upstream's
/// `bdecode()` signature, merged into one typed error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{kind:?} at offset {position}: {}", kind.message())]
pub struct BdecodeError {
    /// The upstream `bdecode_errors` code.
    pub kind: BdecodeErrorKind,
    /// Byte offset where parsing failed (`error_pos`).
    pub position: usize,
}

/// Default parse limits (`bdecode()` defaults).
/// `depth_limit = 100`, `token_limit = 2'000'000`.
pub const DEFAULT_DEPTH_LIMIT: usize = 100;
/// See [`DEFAULT_DEPTH_LIMIT`].
pub const DEFAULT_TOKEN_LIMIT: usize = 2_000_000;
