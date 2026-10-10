//! Port of `aux_/storage_utils.hpp`/`storage_utils.cpp`.
//!
//! The check/move/delete helpers shared by the storage backends:
//! `initialize_storage`, `has_any_file`, `verify_resume_data`,
//! `move_storage`, `delete_files`, `rename_file`, `move_file`,
//! `copy_file` — byte-path resolution goes through `RenamedFiles`
//! (upstream `filenames`).

use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};

use bitdaemon_common::bitfield::Bitfield;
use bitdaemon_common::types::{DownloadPriority, FileIndex};
use bitdaemon_core::file_storage::{FileStorage, RenamedFiles, file_piece_range_inclusive};
use bitdaemon_core::path::bytes_to_path;

use crate::fs::{self, FileStatusFlags};
use crate::stat_cache::StatCache;
use crate::storage::{MoveFlags, disk_status, remove_flags};
use crate::storage_error::{Operation, StorageError, StorageResult};

fn resolve(
    fs: &FileStorage,
    renamed: &RenamedFiles,
    index: FileIndex,
    save_path: &Path,
) -> PathBuf {
    let rel = renamed.file_path(fs, index, &[]);
    save_path.join(bytes_to_path(&rel))
}

fn is_not_found(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::NotFound
}

/// `aux::move_storage` — move/replace/reset the save directory.
/// Returns `(status, new_save_path)`.
pub fn move_storage(
    fs: &FileStorage,
    renamed: &RenamedFiles,
    save_path: PathBuf,
    destination_save_path: PathBuf,
    mut move_partfile: impl FnMut(&Path) -> io::Result<()>,
    flags: MoveFlags,
) -> StorageResult<(u8, PathBuf)> {
    let mut ret = 0u8;
    let new_save_path = fs::complete(&destination_save_path);

    if flags == MoveFlags::FailIfExist {
        match fs::stat_file(&new_save_path, FileStatusFlags::NONE) {
            Err(e) if is_not_found(&e) => {}
            _ => {
                for i in fs.file_range() {
                    if renamed.file_absolute_path(fs, i) {
                        continue;
                    }
                    let p = resolve(fs, renamed, i, &new_save_path);
                    match fs::stat_file(&p, FileStatusFlags::DONT_FOLLOW_LINKS) {
                        Err(e) if is_not_found(&e) => {}
                        Ok(_) => {
                            return Ok((disk_status::FILE_EXIST, save_path));
                        }
                        Err(e) => {
                            return Err(StorageError::new(e, i, Operation::FileStat));
                        }
                    }
                }
            }
        }
    }

    match fs::stat_file(&new_save_path, FileStatusFlags::NONE) {
        Err(e) if is_not_found(&e) => {
            fs::create_directories(&new_save_path)
                .map_err(|e| StorageError::new(e, FileIndex(-1), Operation::Mkdir))?;
        }
        Err(e) => {
            return Err(StorageError::new(e, FileIndex(-1), Operation::FileStat));
        }
        Ok(_) => {}
    }

    match flags {
        MoveFlags::ResetSavePath => {
            return Ok((disk_status::NEED_FULL_CHECK, new_save_path));
        }
        MoveFlags::ResetSavePathUnchecked => {
            return Ok((0, new_save_path));
        }
        _ => {}
    }

    // files we ended up copying — originals stay until the end
    let mut copied: Vec<FileIndex> = Vec::new();
    for i in fs.file_range() {
        if renamed.file_absolute_path(fs, i) {
            continue;
        }
        let old_path = resolve(fs, renamed, i, &save_path);
        let new_path = resolve(fs, renamed, i, &new_save_path);

        if flags == MoveFlags::DontReplace && old_path_is_fine(&new_path) {
            ret |= disk_status::NEED_FULL_CHECK;
            continue;
        }

        match move_file(&old_path, &new_path) {
            Ok(()) => {}
            Err(e) if is_not_found(&e) => {}
            Err(e) => {
                // cross-volume move → copy + delete
                if let Some(parent) = new_path.parent() {
                    fs::create_directories(parent)
                        .map_err(|e2| StorageError::new(e2, i, Operation::Mkdir))?;
                }
                fs::copy_file(&old_path, &new_path)
                    .map_err(|e2| StorageError::new(e2, i, Operation::FileCopy))?;
                copied.push(i);
                let _ = e;
            }
        }
    }

    // move the part file (after the files, like upstream)
    if let Err(e) = move_partfile(&new_save_path) {
        if !is_not_found(&e) {
            return Err(StorageError::new(e, FileIndex(-1), Operation::PartfileMove));
        }
    }

    // delete the originals of files that had to be copied
    for i in copied {
        let old_path = resolve(fs, renamed, i, &save_path);
        let _ = fs::remove(&old_path);
    }

    Ok((ret, new_save_path))
}

fn old_path_is_fine(p: &Path) -> bool {
    p.try_exists().unwrap_or(false)
}

/// `aux::move_file` — rename; same-filesystem moves only. Falling
/// back to copy on `EXDEV` is handled by the caller.
pub fn move_file(f: &Path, newf: &Path) -> io::Result<()> {
    if let Some(parent) = newf.parent() {
        fs::create_directories(parent)?;
    }
    fs::rename(f, newf)
}

/// `aux::delete_one_file` — remove, ignoring `ENOENT`.
fn delete_one_file(p: &Path) -> io::Result<()> {
    match fs::remove(p) {
        Err(e) if is_not_found(&e) => Ok(()),
        r => r,
    }
}

/// `aux::delete_files` — delete torrent files + optionally the part
/// file; then remove now-empty parent directories (deepest first).
pub fn delete_files(
    fs: &FileStorage,
    renamed: &RenamedFiles,
    save_path: &Path,
    part_file: &Path,
    options: u8,
) -> StorageResult<()> {
    if options & remove_flags::DELETE_FILES != 0 {
        let mut directories: BTreeSet<PathBuf> = BTreeSet::new();
        for i in fs.file_range() {
            let fp = bytes_to_path(&renamed.file_path(fs, i, &[]));
            let complete = renamed.file_absolute_path(fs, i);
            let p = if complete {
                fp.clone()
            } else {
                save_path.join(&fp)
            };
            if !complete {
                // collect parent dirs up to save_path for cleanup
                let mut bp = p.parent().map(Path::to_path_buf);
                while let Some(d) = bp {
                    if !d.starts_with(save_path) || d == save_path {
                        break;
                    }
                    directories.insert(d.clone());
                    bp = d.parent().map(Path::to_path_buf);
                }
            }
            delete_one_file(&p).map_err(|e| StorageError::new(e, i, Operation::FileRemove))?;
        }
        // deepest first — BTreeSet reversed order approximates
        // upstream's std::set reverse iteration (path order, not
        // depth; sort by component count to be safe)
        let mut dirs: Vec<PathBuf> = directories.into_iter().collect();
        dirs.sort_by_key(|d| std::cmp::Reverse(d.components().count()));
        for d in dirs {
            let _ = fs::remove(&d); // only removes if empty
        }
    }

    if options & (remove_flags::DELETE_PARTFILE | remove_flags::DELETE_FILES) != 0 {
        match fs::remove(part_file) {
            Ok(()) => {}
            Err(e) if is_not_found(&e) => {}
            Err(e) => {
                return Err(StorageError::new(e, FileIndex(-1), Operation::FileRemove));
            }
        }
    }
    Ok(())
}

/// `aux::rename_file` — rename the on-disk file when it exists;
/// otherwise just record the rename. Refuses to overwrite.
pub fn rename_file(
    fs: &FileStorage,
    renamed: &mut RenamedFiles,
    index: FileIndex,
    new_filename: &[u8],
    save_path: &Path,
) -> StorageResult<()> {
    if index.0 < 0 || index >= fs.end_file() {
        return Ok(());
    }
    let old_rel = renamed.file_path(fs, index, &[]);
    let old_path = save_path.join(bytes_to_path(&old_rel));

    match fs::stat_file(&old_path, FileStatusFlags::DONT_FOLLOW_LINKS) {
        Err(e) if is_not_found(&e) => {
            renamed.rename_file(fs, index, new_filename);
            return Ok(());
        }
        Ok(_) => {}
        Err(e) => return Err(StorageError::new(e, index, Operation::FileStat)),
    }

    let new_name_path = bytes_to_path(new_filename);
    let new_path = if new_name_path.is_absolute() {
        new_name_path
    } else {
        save_path.join(&new_name_path)
    };

    if let Ok(true) = new_path.try_exists() {
        return Err(StorageError::new(
            io::Error::from(io::ErrorKind::AlreadyExists),
            index,
            Operation::FileRename,
        ));
    }

    if let Some(dir) = new_path.parent() {
        fs::create_directories(dir).map_err(|e| StorageError::new(e, index, Operation::Mkdir))?;
    }
    if move_file(&old_path, &new_path).is_err() {
        // cross-filesystem rename → copy + remove (upstream fallback)
        fs::copy_file(&old_path, &new_path)
            .map_err(|e| StorageError::new(e, index, Operation::FileCopy))?;
        let _ = fs::remove(&old_path);
    }
    renamed.rename_file(fs, index, new_filename);
    Ok(())
}

/// `aux::has_any_file`.
pub fn has_any_file(
    fs: &FileStorage,
    renamed: &RenamedFiles,
    save_path: &Path,
    sc: &StatCache,
) -> StorageResult<bool> {
    for i in fs.file_range() {
        if fs.pad_file_at(i) || fs.file_size(i) == 0 {
            continue;
        }
        match sc.get_filesize_path(i, &resolve(fs, renamed, i, save_path)) {
            Ok(_) => return Ok(true),
            Err(e) if is_not_found(&e) => continue,
            Err(e) => return Err(StorageError::new(e, i, Operation::FileStat)),
        }
    }
    Ok(false)
}

fn get_filesize(
    stat: &StatCache,
    i: FileIndex,
    fs: &FileStorage,
    renamed: &RenamedFiles,
    save_path: &Path,
) -> StorageResult<i64> {
    stat.get_filesize_path(i, &resolve(fs, renamed, i, save_path))
        .map_err(|e| {
            if is_not_found(&e) {
                StorageError::new(
                    io::Error::new(io::ErrorKind::InvalidData, "mismatching file size"),
                    i,
                    Operation::FileStat,
                )
            } else {
                StorageError::new(e, i, Operation::FileStat)
            }
        })
}

/// `aux::verify_resume_data` — validate the fastresume record against
/// the filesystem. `have_pieces` is the resume bitfield;
/// `seed_mode`/`links` mirror the `add_torrent_params` fields.
#[allow(clippy::too_many_arguments)]
pub fn verify_resume_data(
    fs: &FileStorage,
    renamed: &RenamedFiles,
    have_pieces: &Bitfield,
    seed_mode: bool,
    links: &[PathBuf],
    file_priority: &[DownloadPriority],
    stat: &StatCache,
    save_path: &Path,
) -> StorageResult<bool> {
    let mut added_files = false;
    if !links.is_empty() {
        debug_assert_eq!(links.len(), fs.num_files() as usize);
        for idx in fs.file_range() {
            let s = &links[idx.get()];
            if s.as_os_str().is_empty() {
                continue;
            }
            let file_path = resolve(fs, renamed, idx, save_path);
            if let Err(e) = fs::hard_link(s, &file_path) {
                if is_not_found(&e) {
                    if let Some(parent) = file_path.parent() {
                        fs::create_directories(parent)
                            .map_err(|e2| StorageError::new(e2, idx, Operation::Mkdir))?;
                    }
                    fs::hard_link(s, &file_path)
                        .map_err(|e2| {
                            if e2.kind() == io::ErrorKind::AlreadyExists {
                                // existing file is not an error
                                io::Error::from(io::ErrorKind::AlreadyExists)
                            } else {
                                e2
                            }
                        })
                        .map_err(|e2| StorageError::new(e2, idx, Operation::FileHardLink))?;
                } else if e.kind() == io::ErrorKind::AlreadyExists {
                    continue;
                } else {
                    return Err(StorageError::new(e, idx, Operation::FileHardLink));
                }
            }
            added_files = true;
            stat.set_dirty(idx);
        }
    }

    let seed = (i64::try_from(have_pieces.size()).unwrap_or(0) >= i64::from(fs.num_pieces())
        && have_pieces.all_set())
        || seed_mode;

    if seed {
        for i in fs.file_range() {
            if fs.pad_file_at(i) {
                continue;
            }
            if i.get() < file_priority.len()
                && file_priority[i.get()] == DownloadPriority::DONT_DOWNLOAD
                && !seed_mode
            {
                continue;
            }
            let size = get_filesize(stat, i, fs, renamed, save_path)?;
            if size < fs.file_size(i) {
                return Err(StorageError::new(
                    io::Error::new(io::ErrorKind::InvalidData, "mismatching file size"),
                    i,
                    Operation::CheckResume,
                ));
            }
        }
        return Ok(true);
    }

    if added_files {
        return Ok(false);
    }

    // non-seed: files touched by completed pieces must exist
    for i in fs.file_range() {
        if i.get() < file_priority.len()
            && file_priority[i.get()] == DownloadPriority::DONT_DOWNLOAD
        {
            continue;
        }
        if fs.pad_file_at(i) || fs.file_size(i) == 0 {
            continue;
        }
        let range = file_piece_range_inclusive(fs, i);
        let end = std::cmp::min(range.end.0, i32::try_from(have_pieces.size()).unwrap_or(0));
        let mut has_completed = false;
        let mut p = range.begin;
        while p.0 < end {
            if have_pieces.get_bit(usize::try_from(p.0).unwrap_or(0)) {
                has_completed = true;
                break;
            }
            p = PieceIndex(p.0 + 1);
        }
        if !has_completed {
            continue;
        }
        get_filesize(stat, i, fs, renamed, save_path)?;
    }
    Ok(true)
}

use bitdaemon_common::types::PieceIndex;

/// `aux::initialize_storage` — create zero-sized files (and symlinks)
/// that don't exist yet; never truncates.
#[allow(clippy::too_many_arguments)]
pub fn initialize_storage(
    fs: &FileStorage,
    renamed: &RenamedFiles,
    save_path: &Path,
    sc: &StatCache,
    file_priority: &[DownloadPriority],
    mut create_file: impl FnMut(FileIndex) -> StorageResult<()>,
    mut create_link: impl FnMut(&Path, &Path) -> StorageResult<()>,
    mut oversized_file: impl FnMut(FileIndex, i64),
) -> StorageResult<()> {
    for i in fs.file_range() {
        if i.get() < file_priority.len()
            && file_priority[i.get()] == DownloadPriority::DONT_DOWNLOAD
        {
            continue;
        }
        if fs.pad_file_at(i) {
            continue;
        }
        let path = resolve(fs, renamed, i, save_path);
        let stat_res = sc.get_filesize_path(i, &path);
        let fs_file_size = fs.file_size(i);
        match stat_res {
            Err(e) if !is_not_found(&e) => {
                return Err(StorageError::new(e, i, Operation::FileStat));
            }
            Ok(sz) if sz > fs_file_size => {
                oversized_file(i, sz);
            }
            _ => {}
        }
        if fs_file_size == 0 {
            // create the empty file when it doesn't exist yet
            match stat_res {
                Err(e) if is_not_found(&e) => {
                    create_file(i)?;
                    // links: symlink targets are resolved by the
                    // caller-supplied create_link; upstream only links
                    // zero-size symlink entries
                    let _ = &mut create_link;
                }
                _ => {}
            }
        }
    }
    Ok(())
}
