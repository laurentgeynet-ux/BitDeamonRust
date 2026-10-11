// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `aux_/xml_parse.cpp` — libtorrent's minimal XML scanner
//! (RSS feeds, UPnP SOAP replies, I2P SAM config). Byte-level cursor
//! port: same token sequence, same error strings. The input is a
//! byte string; upstream operates on `char*` and reports slices —
//! we report `&[u8]` and let callers UTF-8-decode if they care.

/// upstream `xml_*` token kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum XmlToken {
    /// `xml_start_tag` — `<name ...>`.
    StartTag,
    /// `xml_end_tag` — `</name>`.
    EndTag,
    /// `xml_empty_tag` — `<name .../>`.
    EmptyTag,
    /// `xml_declaration_tag` — `<?name ...?>`.
    DeclarationTag,
    /// `xml_string` — character data between tags.
    String,
    /// `xml_attribute` — `name="value"` inside a tag.
    Attribute,
    /// `xml_comment` — `<!-- ... -->`.
    Comment,
    /// `xml_parse_error` — `name` carries the upstream message.
    ParseError,
    /// `xml_tag_content` — non key-value content inside brackets
    /// (e.g. `<!DOCTYPE ...>`).
    TagContent,
}

fn is_space(c: u8) -> bool {
    c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' || c == 0x0b || c == 0x0c
}

/// `string_begins_no_case`.
fn begins_no_case(prefix: &[u8], s: &[u8]) -> bool {
    s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix)
}

/// `xml_parse` — `callback(token, name, value)`; `value` is the
/// attribute value for `Attribute`, empty elsewhere.
pub fn xml_parse(input: &[u8], mut callback: impl FnMut(XmlToken, &[u8], &[u8])) {
    let end = input.len();
    let mut p = 0usize;
    while p < end {
        // look for tag start
        let start0 = p;
        while p < end && input[p] != b'<' {
            p += 1;
        }
        if p > start0 {
            callback(XmlToken::String, &input[start0..p], &[]);
        }
        if p == end {
            break;
        }
        // skip '<'
        p += 1;
        if p < end && p + 8 < end && begins_no_case(b"![CDATA[", &input[p..]) {
            // CDATA: match '![CDATA['
            p += 8;
            let start = p;
            while p < end && !(p >= 2 && begins_no_case(b"]]>", &input[p - 2..])) {
                p += 1;
            }
            if p == end {
                callback(XmlToken::ParseError, b"unexpected end of file", &[]);
                break;
            }
            callback(XmlToken::String, &input[start..p - 2], &[]);
            p += 1; // upstream for-loop `++p` on continue
            continue;
        }

        // tag name: run to '>' or whitespace
        let mut start = p;
        while p < end && input[p] != b'>' && !is_space(input[p]) {
            p += 1;
        }
        let tag_name_end = p;
        // skip attributes for now
        while p < end && input[p] != b'>' {
            p += 1;
        }
        if p == end {
            callback(XmlToken::ParseError, b"unexpected end of file", &[]);
            break;
        }
        debug_assert_eq!(input[p], b'>');
        let mut tag_end = p;
        if input[start] == b'/' {
            start += 1;
            callback(XmlToken::EndTag, &input[start..tag_name_end], &[]);
        } else if input[p - 1] == b'/' {
            let n = tag_name_end.min(p - 1);
            callback(XmlToken::EmptyTag, &input[start..n], &[]);
            tag_end = p - 1;
        } else if input[start] == b'?' && input[p - 1] == b'?' {
            if start == p - 1 {
                callback(XmlToken::ParseError, b"invalid declaration tag", &[]);
                break;
            }
            start += 1;
            let n = tag_name_end.min(p - 1);
            callback(XmlToken::DeclarationTag, &input[start..n], &[]);
            tag_end = p - 1;
        } else if p - start >= 3 && &input[start..start + 3] == b"!--" {
            if start + 5 > p || &input[p - 2..p] != b"--" {
                callback(XmlToken::ParseError, b"invalid comment", &[]);
                break;
            }
            start += 3;
            callback(XmlToken::Comment, &input[start..p - 2], &[]);
            p += 1;
            continue;
        } else {
            callback(XmlToken::StartTag, &input[start..tag_name_end], &[]);
        }

        // parse attributes between the name end and tag end
        let mut i = tag_name_end;
        while i < tag_end {
            while i < tag_end && is_space(input[i]) {
                i += 1;
            }
            if i == tag_end {
                break;
            }
            let astart = i;
            while i < tag_end && input[i] != b'=' && !is_space(input[i]) {
                i += 1;
            }
            let name_len = i - astart;
            // look for '='
            while i < tag_end && input[i] != b'=' {
                i += 1;
            }
            if i == tag_end {
                // bareword — `xml_tag_content` (e.g. DOCTYPE)
                callback(XmlToken::TagContent, &input[astart..i], &[]);
                break;
            }
            i += 1;
            while i < tag_end && is_space(input[i]) {
                i += 1;
            }
            if i == tag_end || (input[i] != b'\'' && input[i] != b'"') {
                callback(XmlToken::ParseError, b"unquoted attribute value", &[]);
                break;
            }
            let quote = input[i];
            i += 1;
            let val_start = i;
            while i < tag_end && input[i] != quote {
                i += 1;
            }
            if i == tag_end {
                callback(XmlToken::ParseError, b"missing end quote on attribute", &[]);
                break;
            }
            callback(
                XmlToken::Attribute,
                &input[astart..astart + name_len],
                &input[val_start..i],
            );
            i += 1; // upstream for-loop `++i` past the closing quote
        }
        p += 1; // upstream for-loop `++p` past the '>'
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(s: &[u8]) -> Vec<(XmlToken, Vec<u8>, Vec<u8>)> {
        let mut v = Vec::new();
        xml_parse(s, |t, n, val| v.push((t, n.to_vec(), val.to_vec())));
        v
    }

    #[test]
    fn doc_parse() {
        let v = collect(b"<a x=\"1\">text</a>");
        assert_eq!(
            v,
            vec![
                (XmlToken::StartTag, b"a".to_vec(), vec![]),
                (XmlToken::Attribute, b"x".to_vec(), b"1".to_vec()),
                (XmlToken::String, b"text".to_vec(), vec![]),
                (XmlToken::EndTag, b"a".to_vec(), vec![]),
            ]
        );
    }

    #[test]
    fn empty_and_decl() {
        let v = collect(b"<?xml version=\"1.0\"?><br/>");
        assert_eq!(v[0].0, XmlToken::DeclarationTag);
        assert_eq!(&v[0].1, b"xml");
        assert_eq!(v[1].0, XmlToken::Attribute);
        assert_eq!(v[2].0, XmlToken::EmptyTag);
    }

    #[test]
    fn comment_and_cdata() {
        let v = collect(b"<!-- hi --><a><![CDATA[<raw>]]></a>");
        assert_eq!(v[0], (XmlToken::Comment, b" hi ".to_vec(), vec![]));
        assert_eq!(v[2], (XmlToken::String, b"<raw>".to_vec(), vec![]));
    }

    #[test]
    fn errors() {
        let v = collect(b"<a");
        assert_eq!(v.last().unwrap().0, XmlToken::ParseError);
        let v = collect(b"<a x=1>");
        assert!(v.iter().any(|(t, _, _)| *t == XmlToken::ParseError));
        let v = collect(b"<a x=\"1>");
        assert!(
            v.iter()
                .any(|(t, n, _)| *t == XmlToken::ParseError
                    && n == b"missing end quote on attribute")
        );
    }

    #[test]
    fn doctype_tag_content() {
        let v = collect(b"<!DOCTYPE html>");
        assert!(
            v.iter()
                .any(|(t, n, _)| *t == XmlToken::TagContent && n == b"html")
        );
    }
}
