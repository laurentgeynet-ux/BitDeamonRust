//! Pretty-printing — port of `print_entry(bdecode_node)` +
//! `entry::to_string` (`bdecode.cpp` and `entry.cpp`).
//!
//! Output shape is identical: `'str'` for printable strings, `xx` hex escape
//! pairs for binary, `123` ints, `[ … ]` / `{ … }` containers, one-line if
//! the whole subtree fits under 200 chars (or `single_line` forced).

use crate::entry::Entry;
use crate::node::{BdecodeNode, NodeType};

/// `aux::escape_string` — printable bytes verbatim, others as `xx` hex.
pub(crate) fn escape_string(out: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let printable = bytes.iter().all(|&c| (32..127).contains(&c));
    if printable {
        out.push_str(std::str::from_utf8(bytes).unwrap_or_default());
    } else {
        for &b in bytes {
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0xf) as usize] as char);
        }
    }
}

/// `print_string` — quoted, truncated for single-line output.
pub(crate) fn print_string(out: &mut String, bytes: &[u8], single_line: bool) {
    let printable = bytes.iter().all(|&c| (32..127).contains(&c));
    out.push('\'');
    if printable {
        if single_line && bytes.len() > 30 {
            out.push_str(std::str::from_utf8(&bytes[..14]).unwrap_or_default());
            out.push_str("...");
            out.push_str(std::str::from_utf8(&bytes[bytes.len() - 14..]).unwrap_or_default());
        } else {
            out.push_str(std::str::from_utf8(bytes).unwrap_or_default());
        }
        out.push('\'');
        return;
    }
    if single_line && bytes.len() > 32 {
        escape_string(out, &bytes[..25]);
        out.push_str("...");
        escape_string(out, &bytes[bytes.len() - 4..]);
    } else {
        escape_string(out, bytes);
    }
    out.push('\'');
}

fn line_longer_than_node(e: &BdecodeNode<'_>, limit: i64) -> i64 {
    let mut line_len: i64 = 0;
    match e.node_type() {
        NodeType::List => {
            line_len += 4;
            if line_len > limit {
                return -1;
            }
            for i in 0..e.list_size() {
                let ret = line_longer_than_node(&e.list_at(i), limit - line_len);
                if ret == -1 {
                    return -1;
                }
                line_len += ret + 2;
            }
        }
        NodeType::Dict => {
            line_len += 4;
            if line_len > limit {
                return -1;
            }
            for i in 0..e.dict_size() {
                let (key, value) = e.dict_at(i);
                line_len += 4 + key.len() as i64;
                if line_len > limit {
                    return -1;
                }
                let ret = line_longer_than_node(&value, limit - line_len);
                if ret == -1 {
                    return -1;
                }
                line_len += ret + 1;
            }
        }
        NodeType::String => {
            line_len += 3 + e.string_length() as i64;
        }
        NodeType::Int => {
            let mut val = e.int_value();
            while val > 0 {
                line_len += 1;
                val /= 10;
            }
            line_len += 2;
        }
        NodeType::None => {
            line_len += 4;
        }
    }
    if line_len > limit {
        return -1;
    }
    line_len
}

fn line_longer_than_entry(e: &Entry, limit: i64) -> i64 {
    let mut line_len: i64 = 0;
    match e {
        Entry::List(l) => {
            line_len += 4;
            if line_len > limit {
                return -1;
            }
            for i in l {
                let ret = line_longer_than_entry(i, limit - line_len);
                if ret == -1 {
                    return -1;
                }
                line_len += ret + 2;
            }
        }
        Entry::Dict(d) => {
            line_len += 4;
            if line_len > limit {
                return -1;
            }
            for (k, v) in d {
                line_len += 4 + k.len() as i64;
                if line_len > limit {
                    return -1;
                }
                let ret = line_longer_than_entry(v, limit - line_len);
                if ret == -1 {
                    return -1;
                }
                line_len += ret + 1;
            }
        }
        Entry::String(s) => line_len += 3 + s.len() as i64,
        Entry::Preformatted(p) => line_len += 3 + p.len() as i64,
        Entry::Int(v) => {
            let mut val = *v;
            while val > 0 {
                line_len += 1;
                val /= 10;
            }
            line_len += 2;
        }
        Entry::Undefined => line_len += 4,
    }
    if line_len > limit {
        return -1;
    }
    line_len
}

/// Upstream's `indent_str`: `",\n"` + `indent` spaces — used full as item
/// separator and `[1..]` (drops the comma) for leading/trailing newlines.
fn indent_str(indent: usize) -> String {
    let mut s = String::with_capacity(indent + 2);
    s.push(',');
    s.push('\n');
    s.extend(std::iter::repeat_n(' ', indent));
    s
}

/// `print_entry(bdecode_node, single_line, indent)`.
#[must_use]
pub fn print_node(e: &BdecodeNode<'_>, single_line: bool, indent: usize) -> String {
    match e.node_type() {
        NodeType::None => "none".to_string(),
        NodeType::Int => e.int_value().to_string(),
        NodeType::String => {
            let mut ret = String::new();
            print_string(&mut ret, e.string_value(), single_line);
            ret
        }
        NodeType::List => {
            let mut ret = String::from("[");
            let one_liner = line_longer_than_node(e, 200) != -1 || single_line;
            let ind = indent_str(indent);
            if !one_liner {
                ret.push_str(&ind[1..]);
            }
            for i in 0..e.list_size() {
                if i == 0 && one_liner {
                    ret.push(' ');
                }
                ret.push_str(&print_node(&e.list_at(i), single_line, indent + 2));
                if i < e.list_size() - 1 {
                    ret.push_str(if one_liner { ", " } else { &ind });
                } else {
                    ret.push_str(if one_liner { " " } else { &ind[1..] });
                }
            }
            ret.push(']');
            ret
        }
        NodeType::Dict => {
            let mut ret = String::from("{");
            let one_liner = line_longer_than_node(e, 200) != -1 || single_line;
            let ind = indent_str(indent);
            if !one_liner {
                ret.push_str(&ind[1..]);
            }
            for i in 0..e.dict_size() {
                if i == 0 && one_liner {
                    ret.push(' ');
                }
                let (key, value) = e.dict_at(i);
                print_string(&mut ret, key, true);
                ret.push_str(": ");
                ret.push_str(&print_node(&value, single_line, indent + 2));
                if i < e.dict_size() - 1 {
                    ret.push_str(if one_liner { ", " } else { &ind });
                } else {
                    ret.push_str(if one_liner { " " } else { &ind[1..] });
                }
            }
            ret.push('}');
            ret
        }
    }
}

/// `print_entry(entry)` — same output shape for the owned [`Entry`].
#[must_use]
pub fn print_entry(e: &Entry, single_line: bool, indent: usize) -> String {
    match e {
        Entry::Undefined => "none".to_string(),
        Entry::Int(v) => v.to_string(),
        Entry::String(s) => {
            let mut ret = String::new();
            print_string(&mut ret, s, single_line);
            ret
        }
        Entry::Preformatted(p) => {
            // preformatted shows as its raw (bencoded) bytes, escaped
            let mut ret = String::new();
            print_string(&mut ret, p, single_line);
            ret
        }
        Entry::List(l) => {
            let mut ret = String::from("[");
            let one_liner = line_longer_than_entry(e, 200) != -1 || single_line;
            let ind = indent_str(indent);
            if !one_liner {
                ret.push_str(&ind[1..]);
            }
            for (i, item) in l.iter().enumerate() {
                if i == 0 && one_liner {
                    ret.push(' ');
                }
                ret.push_str(&print_entry(item, single_line, indent + 2));
                if i < l.len() - 1 {
                    ret.push_str(if one_liner { ", " } else { &ind });
                } else {
                    ret.push_str(if one_liner { " " } else { &ind[1..] });
                }
            }
            ret.push(']');
            ret
        }
        Entry::Dict(d) => {
            let mut ret = String::from("{");
            let one_liner = line_longer_than_entry(e, 200) != -1 || single_line;
            let ind = indent_str(indent);
            if !one_liner {
                ret.push_str(&ind[1..]);
            }
            for (i, (k, v)) in d.iter().enumerate() {
                if i == 0 && one_liner {
                    ret.push(' ');
                }
                print_string(&mut ret, k, true);
                ret.push_str(": ");
                ret.push_str(&print_entry(v, single_line, indent + 2));
                if i < d.len() - 1 {
                    ret.push_str(if one_liner { ", " } else { &ind });
                } else {
                    ret.push_str(if one_liner { " " } else { &ind[1..] });
                }
            }
            ret.push('}');
            ret
        }
    }
}
