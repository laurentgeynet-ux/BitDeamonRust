//! `resolve_duplicate_filenames.cpp` / `resolve_links.cpp` —
//! collision-safe filename resolution.
//!
//! `resolve_links` (hardlink detection across torrents) is deferred: it
//! needs actual on-disk `stat_file` — lands with the disk layer.

use std::collections::{HashMap, HashSet};

use bitdaemon_common::FileIndex;
use bitdaemon_common::crc32c::crc32c;

use crate::error::{Error, Result, TorrentError};
use crate::escape_string::string_equal_no_case;
use crate::file_storage::{FileFlags, FileStorage};
use crate::path::{combine_path, extension, remove_extension};

const fn sep() -> u8 {
    if cfg!(windows) { b'\\' } else { b'/' }
}

fn crc_lowercase(bytes: &[u8]) -> u32 {
    let mut lower = Vec::with_capacity(bytes.len());
    for &c in bytes {
        lower.push(c.to_ascii_lowercase());
    }
    crc32c(&lower)
}

#[derive(Clone, Copy)]
struct NameEntry {
    /// File index, or negative → `-(paths index) - 1` (a directory).
    idx: i64,
    length: usize,
}

/// `aux::resolve_duplicate_filenames` — fast path: hash-only collision
/// check; falls back to the slow path on any hit. Returns the rename
/// map (`file_index → new name`).
pub fn resolve_duplicate_filenames(
    fs: &FileStorage,
    max_duplicate_filenames: i32,
) -> Result<HashMap<FileIndex, Vec<u8>>> {
    let mut files = HashSet::new();
    fs.all_path_hashes(&mut files);
    for i in fs.file_range() {
        if !files.insert(fs.file_path_hash(i, b"")) {
            return resolve_duplicate_filenames_slow(fs, max_duplicate_filenames);
        }
    }
    Ok(HashMap::new())
}

/// Upstream `resolve_duplicate_filenames_slow` — full name comparisons
/// with `.N` suffix disambiguation.
fn resolve_duplicate_filenames_slow(
    fs: &FileStorage,
    max_duplicate_filenames: i32,
) -> Result<HashMap<FileIndex, Vec<u8>>> {
    // hash → name_entry; negative idx indexes `paths`
    let mut files: HashMap<u32, Vec<NameEntry>> = HashMap::new();
    let mut ret: HashMap<FileIndex, Vec<u8>> = HashMap::new();

    let paths = fs.paths();
    // insert all directories first — no file may collide with them
    {
        let base = {
            let mut buf = fs.name().to_vec();
            for c in buf.iter_mut() {
                *c = c.to_ascii_lowercase();
            }
            buf
        };
        for (p_idx, path) in paths.iter().enumerate() {
            let mut buf = base.clone();
            if !path.is_empty() {
                buf.push(sep());
            }
            for (count, &c) in path.iter().enumerate() {
                if c == sep() {
                    files.entry(crc32c(&buf)).or_default().push(NameEntry {
                        idx: -(p_idx as i64) - 1,
                        length: count,
                    });
                }
                buf.push(c.to_ascii_lowercase());
            }
            files.entry(crc32c(&buf)).or_default().push(NameEntry {
                idx: -(p_idx as i64) - 1,
                length: path.len(),
            });
        }
    }

    let mut num_collisions = 0i32;
    for i in fs.file_range() {
        let hash = fs.file_path_hash(i, b"");
        let this_name = fs.file_path(i, b"");
        let candidates = files.get(&hash);

        let matched = candidates.and_then(|cands| {
            cands.iter().find(|e| {
                if e.idx < 0 {
                    let dir = &paths[(-e.idx - 1) as usize][..e.length];
                    string_equal_no_case(&combine_path(fs.name(), dir), &this_name)
                } else {
                    let other = FileIndex(e.idx as i32);
                    match ret.get(&other) {
                        Some(renamed) => string_equal_no_case(renamed, &this_name),
                        None => string_equal_no_case(&fs.file_path(other, b""), &this_name),
                    }
                }
            })
        });

        let Some(m) = matched else {
            files.entry(hash).or_default().push(NameEntry {
                idx: i64::from(i.0),
                length: 0,
            });
            continue;
        };

        // pad files may collide if same size
        let other_idx = m.idx;
        if other_idx >= 0
            && fs.file_flags(i).contains(FileFlags::PAD_FILE)
            && fs
                .file_flags(FileIndex(other_idx as i32))
                .contains(FileFlags::PAD_FILE)
            && fs.file_size(i) == fs.file_size(FileIndex(other_idx as i32))
        {
            continue;
        }

        let base = remove_extension(&this_name).to_vec();
        let ext = extension(&this_name).to_vec();
        let mut cnt = 0i32;
        loop {
            cnt += 1;
            let mut filename = base.clone();
            filename.extend_from_slice(format!(".{cnt}").as_bytes());
            filename.extend_from_slice(&ext);
            let new_hash = crc_lowercase(&filename);
            if !files.contains_key(&new_hash) {
                files.entry(new_hash).or_default().push(NameEntry {
                    idx: i64::from(i.0),
                    length: 0,
                });
                ret.insert(i, filename);
                break;
            }
            num_collisions += 1;
            if num_collisions > max_duplicate_filenames {
                return Err(Error::Torrent(TorrentError::TooManyDuplicateFilenames));
            }
        }
    }
    Ok(ret)
}
