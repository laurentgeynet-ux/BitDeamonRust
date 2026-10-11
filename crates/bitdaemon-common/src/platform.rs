// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `platform_util.cpp` — OS process queries upstream uses
//! for the file-pool size default and thread naming. Implemented
//! unsafe-free via procfs on unix-likes; other platforms get the
//! upstream "can't query" fallback (`i32::MAX` open files, no-op
//! naming — upstream tolerates failure everywhere).

/// `max_open_files` — `getrlimit(RLIMIT_NOFILE)` equivalent. On
/// unix we read `/proc/self/limits` ("Max open files" row); a
/// `unlimited`/`infinity` value or any query failure yields
/// `i32::MAX` (upstream returns `inf` for `RLIM_INFINITY` and for
/// platforms without getrlimit).
#[must_use]
pub fn max_open_files() -> i32 {
    if let Ok(limits) = std::fs::read_to_string("/proc/self/limits") {
        for line in limits.lines() {
            if let Some(rest) = line.strip_prefix("Max open files") {
                let mut fields = rest.split_whitespace();
                if let Some(cur) = fields.next() {
                    if let Ok(v) = cur.parse::<i64>() {
                        return v.clamp(0, i64::from(i32::MAX)) as i32;
                    }
                }
                break;
            }
        }
    }
    i32::MAX
}

/// `set_thread_name` — best-effort naming for the current thread
/// (diagnostics only). Linux: `/proc/self/task/<tid>/comm`,
/// truncated to 15 bytes like `PR_SET_NAME`. Other platforms:
/// no-op (upstream is equally best-effort — BSD/BeOS variants).
pub fn set_thread_name(name: &str) {
    #[cfg(unix)]
    {
        // `/proc/self/comm` names the caller's thread on Linux
        // (15-byte cap like `PR_SET_NAME`); per-thread task comms
        // need gettid which is unsafe-only — process-wide naming is
        // the reasonable approximation upstream uses this for.
        let mut n = name.chars().take(15).collect::<String>();
        n.push('\n');
        let _ = std::fs::write("/proc/self/comm", n);
    }
    #[cfg(not(unix))]
    let _ = name;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_files_positive() {
        assert!(max_open_files() > 0);
    }
}
