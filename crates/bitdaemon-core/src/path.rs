//! `aux_/path.hpp` + `src/path.cpp` — pure path-string helpers.
//!
//! All functions operate on raw bytes: torrent paths are `std::string`
//! byte strings upstream and must survive non-UTF-8 names round-tripping.
//! `TORRENT_SEPARATOR` is `\\` on Windows (`/` is also accepted as an
//! alternate separator there) and `/` elsewhere — same `cfg` split as
//! upstream.

use crate::utf8::{filter_path_character, parse_utf8_codepoint, valid_path_character};

/// `TORRENT_SEPARATOR` as a char.
#[cfg(windows)]
pub const SEPARATOR: u8 = b'\\';
/// `TORRENT_SEPARATOR` as a char.
#[cfg(not(windows))]
pub const SEPARATOR: u8 = b'/';

/// `aux::rsplit_path` — split `p` into `(branch, leaf)` at the last
/// separator. Trailing separators on `p` are ignored.
#[must_use]
pub fn rsplit_path(p: &[u8]) -> (&[u8], &[u8]) {
    if p.is_empty() {
        return (b"", b"");
    }
    let mut p = p;
    if p.last() == Some(&SEPARATOR) || (cfg!(windows) && p.last() == Some(&b'/')) {
        p = &p[..p.len() - 1];
    }
    let sep = if cfg!(windows) {
        p.iter().rposition(|&c| c == b'/' || c == b'\\')
    } else {
        p.iter().rposition(|&c| c == SEPARATOR)
    };
    match sep {
        None => (b"", p),
        Some(i) => (&p[..i], &p[i + 1..]),
    }
}

/// `aux::lsplit_path` — split `p` into `(first_element, rest)`.
/// A leading separator is skipped (absolute paths).
#[must_use]
pub fn lsplit_path(p: &[u8]) -> (&[u8], &[u8]) {
    if p.is_empty() {
        return (b"", b"");
    }
    let mut p = p;
    if p[0] == SEPARATOR || (cfg!(windows) && p[0] == b'/') {
        p = &p[1..];
    }
    let sep = if cfg!(windows) {
        p.iter().position(|&c| c == b'/' || c == b'\\')
    } else {
        p.iter().position(|&c| c == SEPARATOR)
    };
    match sep {
        None => (p, b""),
        Some(i) => (&p[..i], &p[i + 1..]),
    }
}

/// `aux::lsplit_path(p, pos)` — split at the first separator at or after
/// byte offset `pos`.
#[must_use]
pub fn lsplit_path_at(p: &[u8], mut pos: usize) -> (&[u8], &[u8]) {
    if p.is_empty() {
        return (b"", b"");
    }
    let mut p = p;
    if p[0] == SEPARATOR || (cfg!(windows) && p[0] == b'/') {
        p = &p[1..];
        pos = pos.saturating_sub(1);
    }
    let sep = if cfg!(windows) {
        p.iter()
            .skip(pos)
            .position(|&c| c == b'/' || c == b'\\')
            .map(|i| i + pos)
    } else {
        p.iter()
            .skip(pos)
            .position(|&c| c == SEPARATOR)
            .map(|i| i + pos)
    };
    match sep {
        None => (p, b""),
        Some(i) => (&p[..i], &p[i + 1..]),
    }
}

/// `aux::extension` — `".ext"` (with dot) or empty.
#[must_use]
pub fn extension(f: &[u8]) -> &[u8] {
    for i in (0..f.len()).rev() {
        let c = f[i];
        if c == b'/' || (cfg!(windows) && c == b'\\') {
            break;
        }
        if c == b'.' {
            return &f[i..];
        }
    }
    b""
}

/// `aux::remove_extension`.
#[must_use]
pub fn remove_extension(f: &[u8]) -> &[u8] {
    let slash = if cfg!(windows) {
        f.iter().rposition(|&c| c == b'/' || c == b'\\')
    } else {
        f.iter().rposition(|&c| c == b'/')
    };
    let ext = f.iter().rposition(|&c| c == b'.');
    // no extension → return f whole
    match ext {
        None => f,
        Some(0) => f,
        Some(e) if slash.is_some_and(|s| e < s) => f,
        Some(e) => &f[..e],
    }
}

/// `aux::is_root_path`.
#[must_use]
pub fn is_root_path(f: &[u8]) -> bool {
    if f.is_empty() {
        return false;
    }

    #[cfg(windows)]
    {
        // match \\ form
        if f == b"\\\\" {
            return true;
        }
        // match the xx:\ or xx:/ form
        let mut i = 0;
        while i < f.len() && f[i].is_ascii_alphabetic() {
            i += 1;
        }
        if i + 2 == f.len() && f[i] == b':' && (f[i + 1] == b'\\' || f[i + 1] == b'/') {
            return true;
        }
        // match network paths \\computer_name\ form
        if f.len() > 2 && f[0] == b'\\' && f[1] == b'\\' {
            let found = f[2..f.len() - 1].iter().any(|&c| c == b'\\' || c == b'/');
            if !found {
                return true;
            }
        }
        false
    }

    #[cfg(not(windows))]
    {
        f == b"/"
    }
}

/// `aux::path_equal` — equal ignoring a single trailing separator.
#[must_use]
pub fn path_equal(lhs: &[u8], rhs: &[u8]) -> bool {
    fn trim(p: &[u8]) -> &[u8] {
        if p.last() == Some(&b'/') || (cfg!(windows) && p.last() == Some(&b'\\')) {
            &p[..p.len() - 1]
        } else {
            p
        }
    }
    trim(lhs) == trim(rhs)
}

/// `aux::path_compare` — element-wise comparison; the shorter side's
/// filename substitutes for a missing element.
#[must_use]
pub fn path_compare(lhs: &[u8], lfile: &[u8], rhs: &[u8], rfile: &[u8]) -> std::cmp::Ordering {
    let mut lhs_elems = lsplit_path(lhs);
    let mut rhs_elems = lsplit_path(rhs);
    while !lhs_elems.0.is_empty() || !rhs_elems.0.is_empty() {
        if lhs_elems.0.is_empty() || rhs_elems.0.is_empty() {
            let l = if lhs_elems.0.is_empty() {
                lfile
            } else {
                lhs_elems.0
            };
            let r = if rhs_elems.0.is_empty() {
                rfile
            } else {
                rhs_elems.0
            };
            return l.cmp(r);
        }
        let ord = lhs_elems.0.cmp(rhs_elems.0);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
        lhs_elems = lsplit_path(lhs_elems.1);
        rhs_elems = lsplit_path(rhs_elems.1);
    }
    std::cmp::Ordering::Equal
}

/// `aux::has_parent_path` — true if `f` has a directory component.
#[must_use]
pub fn has_parent_path(f: &[u8]) -> bool {
    if f.is_empty() || is_root_path(f) {
        return false;
    }
    let mut len = f.len() - 1;
    // ignore a trailing / or \
    if f[len] == b'/' || f[len] == b'\\' {
        len = len.wrapping_sub(1);
    }
    loop {
        if len == usize::MAX {
            return false;
        }
        if f[len] == b'/' || f[len] == b'\\' {
            return true;
        }
        len = len.wrapping_sub(1);
    }
}

/// `aux::parent_path`.
#[must_use]
pub fn parent_path(f: &[u8]) -> &[u8] {
    if f.is_empty() {
        return f;
    }
    #[cfg(windows)]
    if f == b"\\\\" {
        return b"";
    }
    if f == b"/" {
        return b"";
    }

    let mut len = f.len();
    // ignore a trailing / or \
    if f[len - 1] == b'/' || (cfg!(windows) && f[len - 1] == b'\\') {
        len -= 1;
    }
    while len > 0 {
        len -= 1;
        let c = f[len];
        if c == b'/' || (cfg!(windows) && c == b'\\') {
            break;
        }
    }
    if len < f.len() && (f[len] == b'/' || (cfg!(windows) && f[len] == b'\\')) {
        len += 1;
    }
    &f[..len]
}

/// `aux::filename` — the leaf component.
#[must_use]
pub fn filename(f: &[u8]) -> &[u8] {
    if f.is_empty() {
        return b"";
    }
    let is_sep = |c: u8| c == b'/' || (cfg!(windows) && c == b'\\');
    let sep = f.iter().rposition(|&c| is_sep(c));
    match sep {
        None => f,
        Some(s) => {
            if s == f.len() - 1 {
                // trailing separator: walk back to the previous one
                let mut len = 0;
                let mut i = s;
                while i > 0 {
                    i -= 1;
                    if is_sep(f[i]) {
                        return &f[i + 1..i + 1 + len];
                    }
                    len += 1;
                }
                &f[..len]
            } else {
                &f[s + 1..]
            }
        }
    }
}

/// `aux::append_path` — append `leaf` to `branch` inserting one separator
/// if needed. `branch` empty or `"."` becomes `leaf`.
pub fn append_path(branch: &mut Vec<u8>, leaf: &[u8]) {
    debug_assert!(!is_complete(leaf));
    if branch.is_empty() || branch.as_slice() == b"." {
        branch.clear();
        branch.extend_from_slice(leaf);
        return;
    }
    if leaf.is_empty() {
        return;
    }
    if branch.last() != Some(&SEPARATOR) && !(cfg!(windows) && branch.last() == Some(&b'/')) {
        branch.push(SEPARATOR);
    }
    branch.extend_from_slice(leaf);
}

/// `aux::combine_path`.
#[must_use]
pub fn combine_path(lhs: &[u8], rhs: &[u8]) -> Vec<u8> {
    debug_assert!(!is_complete(rhs));
    if lhs.is_empty() || lhs == b"." {
        return rhs.to_vec();
    }
    if rhs.is_empty() || rhs == b"." {
        return lhs.to_vec();
    }
    let need_sep = lhs.last() != Some(&SEPARATOR) && !(cfg!(windows) && lhs.last() == Some(&b'/'));
    let mut ret = Vec::with_capacity(lhs.len() + rhs.len() + usize::from(need_sep));
    ret.extend_from_slice(lhs);
    if need_sep {
        ret.push(SEPARATOR);
    }
    ret.extend_from_slice(rhs);
    ret
}

/// `aux::split_string` — `(token, rest)` at first `sep`.
#[must_use]
pub fn split_string(s: &[u8], sep: u8) -> (&[u8], &[u8]) {
    match s.iter().position(|&c| c == sep) {
        None => (s, b""),
        Some(i) => (&s[..i], &s[i + 1..]),
    }
}

/// `aux::lexically_relative` — `target` relative to `base` via `..`.
#[must_use]
pub fn lexically_relative(base: &[u8], target: &[u8]) -> Vec<u8> {
    // strip trailing separators
    let mut base = base;
    let mut target = target;
    if base.last() == Some(&SEPARATOR) {
        base = &base[..base.len() - 1];
    }
    if target.last() == Some(&SEPARATOR) {
        target = &target[..target.len() - 1];
    }

    // strip common path elements
    loop {
        if base.is_empty() {
            break;
        }
        let prev_base = base;
        let prev_target = target;
        let (base_element, b) = split_string(base, SEPARATOR);
        let (target_element, t) = split_string(target, SEPARATOR);
        base = b;
        target = t;
        if base_element == target_element {
            continue;
        }
        base = prev_base;
        target = prev_target;
        break;
    }

    // one ".." per remaining base element
    let num_steps =
        base.iter().filter(|&&c| c == SEPARATOR).count() + usize::from(!base.is_empty());
    let mut ret = Vec::new();
    for _ in 0..num_steps {
        ret.extend_from_slice(b"..");
        ret.push(SEPARATOR);
    }
    ret.extend_from_slice(target);
    ret
}

/// `aux::lexically_relative_normal` — `target` relative to `base`,
/// resolving `.`/`..`; `Err` if `target` escapes `base`.
pub fn lexically_relative_normal(base: &[u8], target: &[u8]) -> Result<Vec<u8>, crate::Error> {
    // strip shared leading components
    let mut b = lsplit_path(base);
    let mut t = lsplit_path(target);
    while !b.0.is_empty() && b.0 == t.0 {
        b = lsplit_path(b.1);
        t = lsplit_path(t.1);
    }

    // leftover in base → target lies outside base
    if !b.0.is_empty() {
        return Err(crate::Error::InvalidEncoding(
            "target lies outside base".to_string(),
        ));
    }

    let mut components: Vec<&[u8]> = Vec::new();
    while !t.0.is_empty() {
        if t.0 == b"." {
            // skip
        } else if t.0 == b".." {
            if components.pop().is_none() {
                return Err(crate::Error::InvalidEncoding(
                    "target escapes base".to_string(),
                ));
            }
        } else {
            components.push(t.0);
        }
        t = lsplit_path(t.1);
    }

    let mut ret = Vec::new();
    for c in components {
        if !ret.is_empty() {
            ret.push(SEPARATOR);
        }
        ret.extend_from_slice(c);
    }
    Ok(ret)
}

/// `aux::is_complete` — absolute path?
#[must_use]
pub fn is_complete(f: &[u8]) -> bool {
    if f.is_empty() {
        return false;
    }
    #[cfg(windows)]
    {
        // match the xx:\ or xx:/ form
        let mut i = 0;
        while i < f.len() && f[i].is_ascii_alphabetic() {
            i += 1;
        }
        if i + 1 < f.len() && f[i] == b':' && (f[i + 1] == b'\\' || f[i + 1] == b'/') {
            return true;
        }
        // match the \\ form
        f.len() >= 2 && f[0] == b'\\' && f[1] == b'\\'
    }
    #[cfg(not(windows))]
    {
        f[0] == b'/'
    }
}

/// `aux::complete` — make `f` absolute w.r.t. the working directory.
#[must_use]
pub fn complete(f: &[u8]) -> Vec<u8> {
    if is_complete(f) {
        return f.to_vec();
    }
    let parts = lsplit_path(f);
    let f = if parts.0 == b"." { parts.1 } else { f };
    let cwd = std::env::current_dir()
        .map(|p| p.to_string_lossy().into_owned().into_bytes())
        .unwrap_or_default();
    combine_path(&cwd, f)
}

/// `aux::current_working_directory`.
#[must_use]
pub fn current_working_directory() -> Vec<u8> {
    std::env::current_dir()
        .map(|p| p.to_string_lossy().into_owned().into_bytes())
        .unwrap_or_default()
}

/// `aux::sanitize_append_path_element` — append `element` to `path`,
/// sanitizing invalid characters (one `_` each) and stripping filter
/// characters. Always adds an element when `force_element` — v2 torrents
/// need a placeholder for every declared path element.
///
/// Transcribed from `torrent_info.cpp` (kept here with the path helpers).
pub fn sanitize_append_path_element(path: &mut Vec<u8>, element: &[u8], force_element: bool) {
    if element.len() == 1 && element[0] == b'.' && !force_element {
        return;
    }

    path.reserve(element.len() + 2);
    let mut added_separator = false;
    if !path.is_empty() {
        path.push(SEPARATOR);
        added_separator = true;
    }

    if element.is_empty() {
        path.push(b'_');
        return;
    }

    let mut added: usize = 0;
    // the number of dots we've added
    let mut num_dots = 0;
    let mut found_extension = false;

    #[cfg(windows)]
    let mut unicode_chars = 0usize;

    let mut i = 0usize;
    while i < element.len() {
        let (code_point, seq_len) = parse_utf8_codepoint(&element[i..]);
        i += seq_len;

        if code_point >= 0 && filter_path_character(code_point) {
            continue;
        }

        if code_point < 0 || !valid_path_character(code_point) {
            // invalid utf8 sequence / invalid character → "_"
            path.push(b'_');
            added += 1;
            #[cfg(windows)]
            {
                unicode_chars += 1;
            }
            continue;
        }

        // validation passed, add the raw sequence bytes
        path.extend_from_slice(&element[i - seq_len..i]);

        if code_point == i32::from(b'.') {
            num_dots += 1;
        }

        added += seq_len;
        #[cfg(windows)]
        {
            unicode_chars += 1;
        }

        // any given path element should not be more than 255 characters;
        // if we exceed 240, pick up any potential file extension
        #[cfg(windows)]
        let limit_hit = unicode_chars >= 240;
        #[cfg(not(windows))]
        let limit_hit = added >= 240;

        if limit_hit && !found_extension {
            // upstream: for (j = size-1; j > max(size-10, i); --j)
            let mut dot: Option<usize> = None;
            let mut j = element.len() as i64 - 1;
            let min_j = std::cmp::max(element.len() as i64 - 10, i as i64);
            while j > min_j {
                if element[j as usize] == b'.' {
                    dot = Some(j as usize);
                    break;
                }
                j -= 1;
            }
            let Some(dot) = dot else {
                break;
            };
            found_extension = true;
            // upstream sets i = dot - seq_len and the for-increment re-adds
            // seq_len; we already advanced `i` at the top of the loop
            i = dot;
        }
    }

    if added == num_dots && added <= 2 {
        if force_element {
            // revert the invalid filename and replace it with an underscore
            let new_len = path.len() - added;
            path.truncate(new_len);
            path.push(b'_');
        } else {
            // revert everything
            let new_len = path.len() - added - usize::from(added_separator);
            path.truncate(new_len);
        }
        return;
    }

    #[cfg(windows)]
    {
        // remove trailing spaces and dots — not allowed on windows
        while let Some(&c) = path.last() {
            if c != b' ' && c != b'.' {
                break;
            }
            path.pop();
            added -= 1;
        }

        if force_element && added == 0 {
            path.push(b'_');
        } else if added == 0 && added_separator {
            // remove the separator added at the beginning
            path.pop();
            return;
        }
    }

    if path.is_empty() {
        path.push(b'_');
    }
}
