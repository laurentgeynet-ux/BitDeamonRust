//! The encoder — port of `bencode()` + `aux::bencode_visitor`
//! (`bencode.hpp`, `entry.cpp`).
//!
//! Output is canonical: dict keys are emitted in `BTreeMap` order
//! (byte-lexicographic — the same order `entry`'s sorted `map` yields, and
//! the order the BitTorrent spec requires). `Preformatted` bytes are copied
//! verbatim; `Undefined` encodes as an empty string (`0:`), like upstream.

use crate::entry::Entry;

/// `aux::write_integer` — decimal ASCII digits.
fn write_integer(out: &mut Vec<u8>, val: i64) -> usize {
    let s = val.to_string();
    out.extend_from_slice(s.as_bytes());
    s.len()
}

/// `bencode_visitor` — appends the bencoding of `e` to `out`,
/// returns bytes written.
pub fn bencode_to(out: &mut Vec<u8>, e: &Entry) -> usize {
    match e {
        Entry::Int(i) => {
            out.push(b'i');
            let ret = write_integer(out, *i);
            out.push(b'e');
            ret + 2
        }
        Entry::String(s) => {
            let mut ret = write_integer(out, s.len() as i64);
            out.push(b':');
            out.extend_from_slice(s);
            ret += s.len();
            ret + 1
        }
        Entry::List(l) => {
            out.push(b'l');
            let mut ret = 2;
            for i in l {
                ret += bencode_to(out, i);
            }
            out.push(b'e');
            ret
        }
        Entry::Dict(d) => {
            out.push(b'd');
            let mut ret = 2;
            for (k, v) in d {
                // key
                ret += write_integer(out, k.len() as i64);
                out.push(b':');
                out.extend_from_slice(k);
                ret += k.len();
                // value (the +1 is the ':' after the length digits)
                ret += bencode_to(out, v) + 1;
            }
            out.push(b'e');
            ret
        }
        Entry::Preformatted(p) => {
            out.extend_from_slice(p);
            p.len()
        }
        Entry::Undefined => {
            // upstream encodes uninitialized as an empty string
            out.extend_from_slice(b"0:");
            2
        }
    }
}

/// `bencode(entry)` — serialize to a fresh buffer.
#[must_use]
pub fn bencode(e: &Entry) -> Vec<u8> {
    let mut out = Vec::new();
    bencode_to(&mut out, e);
    out
}
