//! The bdecode parser — port of `bdecode()` in `libtorrent/src/bdecode.cpp`.
//!
//! Fidelity notes vs. upstream:
//! - The token stream is flat; containers store a relative `next_item` used to
//!   skip whole subtrees and to delimit `data_section`/`int_value`/`string_value`.
//! - On failure the partially-built tree is *back-patched* with `end` tokens
//!   (plus an empty-dict dummy value for a pending dict key) so the result is
//!   still traversable — callers get a node plus an error, like upstream's
//!   `bdecode_node` + `error_code` out-param pair.
//! - Parsing stops after the first complete top-level item; trailing bytes are
//!   ignored (`m_buffer_size` records the consumed length).

use crate::error::{BdecodeError, BdecodeErrorKind, DEFAULT_DEPTH_LIMIT, DEFAULT_TOKEN_LIMIT};
use crate::node::BdecodeNode;
use crate::token::{BdecodeToken, LONG_STRING_MAX_HEADER, MAX_NEXT_ITEM, MAX_OFFSET, TokenKind};

fn numeric(c: u8) -> bool {
    c.is_ascii_digit()
}

/// `stack_frame` — `token` is an index into the token vec, `state` 0/1 marks
/// dict key vs. dict value position.
struct StackFrame {
    token: u32,
    state: u8,
}

/// `check_integer` — validate `i<digits>e` starting at `pos` (just past `i`).
/// Returns the position of the terminating `e`, or the error + the position
/// where scanning stopped (matching upstream's `error_pos`).
fn check_integer(b: &[u8], mut p: usize) -> Result<usize, (BdecodeErrorKind, usize)> {
    if p == b.len() {
        return Err((BdecodeErrorKind::UnexpectedEof, p));
    }

    if b[p] == b'-' {
        p += 1;
        if p == b.len() {
            return Err((BdecodeErrorKind::UnexpectedEof, p));
        }
    }

    let mut digits = 0u32;
    let mut e = None;
    loop {
        if !numeric(b[p]) {
            e = Some(BdecodeErrorKind::ExpectedDigit);
            break;
        }
        p += 1;
        digits += 1;
        if p == b.len() {
            e = Some(BdecodeErrorKind::UnexpectedEof);
            break;
        }
        if b[p] == b'e' {
            break;
        }
    }

    // upstream applies this after the loop — it can override an earlier
    // expected_digit/unexpected_eof error
    if digits > 20 {
        e = Some(BdecodeErrorKind::Overflow);
    }
    match e {
        Some(kind) => Err((kind, p)),
        None => Ok(p),
    }
}

/// `aux::parse_int` — accumulate decimal digits until `delimiter`.
/// `val` must start non-negative (upstream requires 0 or positive).
/// Returns the delimiter position, or the error + where scanning stopped.
pub(crate) fn parse_int(
    b: &[u8],
    mut p: usize,
    delimiter: u8,
    val: &mut i64,
) -> Result<usize, (BdecodeErrorKind, usize)> {
    debug_assert!(*val >= 0);
    while p < b.len() && b[p] != delimiter {
        let c = b[p];
        if !numeric(c) {
            return Err((BdecodeErrorKind::ExpectedDigit, p));
        }
        if *val > i64::MAX / 10 {
            return Err((BdecodeErrorKind::Overflow, p));
        }
        *val *= 10;
        let digit = i64::from(c - b'0');
        if *val > i64::MAX - digit {
            return Err((BdecodeErrorKind::Overflow, p));
        }
        *val += digit;
        p += 1;
    }
    Ok(p)
}

/// `bdecode(buffer, ec, error_pos)` with default limits.
///
/// Always returns a node: on failure `node.error()` is `Some` and the
/// (back-patched, traversable) partial tree is still available — mirroring
/// upstream where the `bdecode_node` outlives `ec`.
#[must_use]
pub fn bdecode(buffer: &[u8]) -> BdecodeNode<'_> {
    bdecode_limits(buffer, DEFAULT_DEPTH_LIMIT, DEFAULT_TOKEN_LIMIT)
}

/// Convenience for callers that only want the result on success —
/// `if (ec) return` upstream. The partial tree is discarded on error.
pub fn bdecode_checked(buffer: &[u8]) -> Result<BdecodeNode<'_>, BdecodeError> {
    let node = bdecode(buffer);
    match node.error() {
        Some(e) => Err(e),
        None => Ok(node),
    }
}

/// `bdecode(buffer, ec, error_pos, depth_limit, token_limit)`.
#[must_use]
pub fn bdecode_limits(buffer: &[u8], depth_limit: usize, token_limit: usize) -> BdecodeNode<'_> {
    bdecode_impl(buffer, buffer.len(), depth_limit, token_limit)
}

/// `claimed_len` stands in for upstream's `span` length: tests can pretend
/// the buffer is larger than it is without allocating `max_offset` bytes.
fn bdecode_impl(
    buffer: &[u8],
    claimed_len: usize,
    depth_limit: usize,
    token_limit: usize,
) -> BdecodeNode<'_> {
    let mut tokens: Vec<BdecodeToken> = Vec::new();
    let mut error: Option<BdecodeError> = None;

    let end = buffer.len();

    // upstream rejects buffers > max_offset up front, returning a *none* node
    if claimed_len > MAX_OFFSET as usize {
        return BdecodeNode::failed(
            buffer,
            BdecodeError {
                kind: BdecodeErrorKind::LimitExceeded,
                position: 0,
            },
        );
    }

    let mut stack: Vec<StackFrame> = Vec::with_capacity(depth_limit.min(1024));
    let mut start = 0usize;
    let mut token_limit = token_limit;

    'parse: loop {
        if start >= end {
            error = Some(BdecodeError {
                kind: BdecodeErrorKind::UnexpectedEof,
                position: start,
            });
            break;
        }

        if stack.len() >= depth_limit {
            error = Some(BdecodeError {
                kind: BdecodeErrorKind::DepthExceeded,
                position: start,
            });
            break;
        }

        if token_limit == 0 {
            error = Some(BdecodeError {
                kind: BdecodeErrorKind::LimitExceeded,
                position: start,
            });
            break;
        }
        token_limit -= 1;

        let t = buffer[start];
        let current_frame = stack.len();

        // if we're currently parsing a dictionary, every other node must
        // be a string (key) — or 'e' to terminate
        if current_frame > 0
            && tokens[stack[current_frame - 1].token as usize].kind() == TokenKind::Dict
            && stack[current_frame - 1].state == 0
            && !numeric(t)
            && t != b'e'
        {
            error = Some(BdecodeError {
                kind: BdecodeErrorKind::ExpectedDigit,
                position: start,
            });
            break;
        }

        match t {
            b'd' => {
                stack.push(StackFrame {
                    token: tokens.len() as u32,
                    state: 0,
                });
                tokens.push(BdecodeToken::new(start as u32, TokenKind::Dict));
                start += 1;
            }
            b'l' => {
                stack.push(StackFrame {
                    token: tokens.len() as u32,
                    state: 0,
                });
                tokens.push(BdecodeToken::new(start as u32, TokenKind::List));
                start += 1;
            }
            b'i' => {
                let int_start = start;
                match check_integer(buffer, start + 1) {
                    Err((kind, pos)) => {
                        // upstream restores `start` to the 'i' so the unwind
                        // code records token offsets there
                        start = int_start;
                        error = Some(BdecodeError {
                            kind,
                            position: pos,
                        });
                        break 'parse;
                    }
                    Ok(p) => start = p,
                }
                tokens.push(BdecodeToken::with_next(
                    int_start as u32,
                    1,
                    TokenKind::Integer,
                    1,
                ));
                debug_assert_eq!(buffer[start], b'e');
                start += 1; // skip 'e'
            }
            b'e' => {
                // end of a list or dict
                let Some(&StackFrame {
                    token: top_tok,
                    state,
                }) = stack.last()
                else {
                    error = Some(BdecodeError {
                        kind: BdecodeErrorKind::UnexpectedEof,
                        position: start,
                    });
                    break;
                };
                if tokens[top_tok as usize].kind() == TokenKind::Dict && state == 1 {
                    // a dict value was expected, got termination instead
                    error = Some(BdecodeError {
                        kind: BdecodeErrorKind::ExpectedValue,
                        position: start,
                    });
                    break;
                }

                // end-of-sequence token
                tokens.push(BdecodeToken::with_next(start as u32, 1, TokenKind::End, 0));

                // back-patch the container's next_item
                let top = top_tok as usize;
                if tokens.len() - top > MAX_NEXT_ITEM as usize {
                    error = Some(BdecodeError {
                        kind: BdecodeErrorKind::LimitExceeded,
                        position: start,
                    });
                    break;
                }
                let next = (tokens.len() - top) as u32;
                tokens[top].set_next_item(next);
                stack.pop();
                start += 1;
            }
            _ => {
                // strings: start character must be a digit
                if !numeric(t) {
                    error = Some(BdecodeError {
                        kind: BdecodeErrorKind::ExpectedValue,
                        position: start,
                    });
                    break;
                }

                let mut len = i64::from(t - b'0');
                let str_start = start;
                start += 1;
                if start >= end {
                    error = Some(BdecodeError {
                        kind: BdecodeErrorKind::UnexpectedEof,
                        position: start,
                    });
                    break;
                }
                match parse_int(buffer, start, b':', &mut len) {
                    Err((kind, pos)) => {
                        error = Some(BdecodeError {
                            kind,
                            position: pos,
                        });
                        break 'parse;
                    }
                    Ok(p) => start = p,
                }
                if start == end {
                    error = Some(BdecodeError {
                        kind: BdecodeErrorKind::ExpectedColon,
                        position: start,
                    });
                    break;
                }

                // remaining buffer size excluding ':'
                let buff_size = end - start - 1;
                if len > buff_size as i64 {
                    error = Some(BdecodeError {
                        kind: BdecodeErrorKind::UnexpectedEof,
                        position: start,
                    });
                    break;
                }
                if len < 0 {
                    error = Some(BdecodeError {
                        kind: BdecodeErrorKind::Overflow,
                        position: start,
                    });
                    break;
                }

                start += 1; // skip ':'
                let header = start - str_start;
                if header as u32 > LONG_STRING_MAX_HEADER {
                    error = Some(BdecodeError {
                        kind: BdecodeErrorKind::LimitExceeded,
                        position: start,
                    });
                    break;
                }

                tokens.push(BdecodeToken::with_next(
                    str_start as u32,
                    1,
                    TokenKind::String,
                    header as u32,
                ));
                start += len as usize;
            }
        }

        // if the parent is a dict, flip key/value state. Upstream indexes
        // `stack[current_frame - 1]` — after an 'e' pop that's the stale
        // frame in their alloca array (a dead write); for our Vec the slot
        // is gone, so `get_mut` returning None is exactly equivalent.
        if current_frame > 0 {
            if let Some(top) = stack.get_mut(current_frame - 1) {
                if tokens[top.token as usize].kind() == TokenKind::Dict {
                    top.state ^= 1;
                }
            }
        }

        // top-level node complete — done
        if stack.is_empty() {
            break;
        }
    }

    // 'done' label: unwind the stack on failure by inserting terminators so
    // whatever was parsed so far forms a valid tree
    while let Some(frame) = stack.pop() {
        debug_assert!(error.is_some());
        // a dict expecting a value gets an empty dict as the value
        if tokens[frame.token as usize].kind() == TokenKind::Dict && frame.state == 1 {
            tokens.push(BdecodeToken::with_next(start as u32, 2, TokenKind::Dict, 0));
            tokens.push(BdecodeToken::new(start as u32, TokenKind::End));
        }
        let top = frame.token as usize;
        debug_assert!(tokens.len() - top <= MAX_NEXT_ITEM as usize);
        let next = (tokens.len() - top) as u32;
        tokens[top].set_next_item(next);
        tokens.push(BdecodeToken::with_next(start as u32, 1, TokenKind::End, 0));
    }

    tokens.push(BdecodeToken::with_next(start as u32, 0, TokenKind::End, 0));

    BdecodeNode::new(tokens, buffer, start, error)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// parse via a fake buffer length — upstream's `bdecode({b, N})` tests
    /// pretend the buffer is bigger than the literal.
    fn bdecode_pretend(buffer: &[u8], claimed_len: usize) -> BdecodeNode<'_> {
        bdecode_impl(
            buffer,
            claimed_len,
            DEFAULT_DEPTH_LIMIT,
            DEFAULT_TOKEN_LIMIT,
        )
    }

    // upstream: exceed_buf_limit
    #[test]
    fn exceed_buf_limit() {
        let e = bdecode_pretend(b"l3:fooe", 0x3fffffff);
        let err = e.error().expect("parse must fail");
        assert_eq!(err.kind, BdecodeErrorKind::LimitExceeded);
        assert_eq!(err.position, 0);
    }

    // upstream: overflow_length_prefix2 — "pretend that we have a large buffer"
    #[test]
    fn overflow_length_prefix2() {
        let e = bdecode_pretend(b"199999999:foobar", 999_999_999);
        let err = e.error().expect("parse must fail");
        assert_eq!(err.kind, BdecodeErrorKind::LimitExceeded);
        assert_eq!(err.position, 0);
    }

    // upstream: parse_int
    #[test]
    fn parse_int_ok() {
        let b = b"1234567890e";
        let mut val = 0i64;
        let e = parse_int(b, 0, b'e', &mut val);
        assert_eq!(e, Ok(10));
        assert_eq!(val, 1234567890);
    }

    // upstream: invalid_digit
    #[test]
    fn parse_int_invalid_digit() {
        let b = b"0o";
        let mut val = 0i64;
        let e = parse_int(b, 0, b'e', &mut val);
        assert_eq!(e, Err((BdecodeErrorKind::ExpectedDigit, 1)));
    }

    // upstream: parse_int_overflow
    #[test]
    fn parse_int_overflow() {
        let b = b"9223372036854775808:";
        let mut val = 0i64;
        let e = parse_int(b, 0, b':', &mut val);
        assert_eq!(e, Err((BdecodeErrorKind::Overflow, 18)));
    }

    // upstream: expected_colon_string
    #[test]
    fn parse_int_eof_is_ok() {
        let b = b"928";
        let mut val = 0i64;
        let e = parse_int(b, 0, b':', &mut val);
        assert_eq!(e, Ok(3));
        assert_eq!(val, 928);
    }
}
