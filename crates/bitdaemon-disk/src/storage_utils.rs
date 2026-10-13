//! Port of `aux_/storage_utils.hpp`/`storage_utils.cpp` and
//! `truncate.cpp`.
//!
//! The check/move/delete helpers shared by the storage backends:
//! `initialize_storage`, `has_any_file`, `verify_resume_data`,
//! `move_storage`, `delete_files`, `rename_file`, `move_file`,
//! `create_symlink`, `truncate_files` — byte-path resolution goes
//! through `RenamedFiles` (upstream `filenames`).
//!
//! Upstream returns a `(status, path)` pair *plus* a `storage_error`
//! out-param — errors and status are independent channels (a
//! `file_exist` verdict still carries the stat error; a fatal move
//! still reports the *old* save path). Rust models that as
//! `(u8, PathBuf, Option<StorageError>)` — an `Err`-as-fatal shortcut
//! would lose the path and flatten `file_exist` into `fatal`.

use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};

use bitdaemon_common::bitfield::Bitfield;
use bitdaemon_common::types::{DownloadPriority, FileIndex, PieceIndex};
use bitdaemon_core::file_storage::{
    FileFlags, FileStorage, RenamedFiles, file_piece_range_inclusive,
};
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

fn is_invalid_argument(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::InvalidInput
        || e.raw_os_error() == Some(22) // EINVAL
        || e.raw_os_error() == Some(87) // ERROR_INVALID_PARAMETER
}

fn is_permission_denied(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::PermissionDenied
}

/// `filename_too_long` across platforms — `io::ErrorKind::InvalidFilename`
/// is Rust 1.87+, past our 1.85 MSRV; match the raw codes instead
/// (ENAMETOOLONG=36 Linux / 63 macOS+BSD; Windows ERROR_INVALID_NAME=123,
/// ERROR_FILENAME_EXCED_RANGE=206).
fn is_filename_too_long(e: &io::Error) -> bool {
    matches!(e.raw_os_error(), Some(36 | 63 | 123 | 206))
}

/// Errors upstream treats as terminal for a rename — not worth a
/// copy fallback (`invalid_argument`, `permission_denied`).
fn is_hard_rename_error(e: &io::Error) -> bool {
    is_invalid_argument(e) || is_permission_denied(e)
}

/// `aux::move_file` — lstat the source (`file_stat` on failure),
/// create the destination's parent (`mkdir`), then `rename`
/// (`file_rename`). Cross-filesystem fallback is the caller's job.
pub fn move_file(f: &Path, newf: &Path) -> StorageResult<()> {
    // lstat first: rename() moves the entry itself, and a stat error
    // (permissions…) must surface as `file_stat`, not be re-labeled
    // by the rename
    fs::stat_file(f, FileStatusFlags::DONT_FOLLOW_LINKS)
        .map_err(|e| StorageError::new(e, FileIndex(-1), Operation::FileStat))?;
    if let Some(parent) = newf.parent()
        && parent != Path::new("")
    {
        fs::create_directories(parent)
            .map_err(|e| StorageError::new(e, FileIndex(-1), Operation::Mkdir))?;
    }
    fs::rename(f, newf).map_err(|e| StorageError::new(e, FileIndex(-1), Operation::FileRename))
}

/// `lexically_relative(parent_path(file), target)` — path-computation
/// helper upstream uses to anchor symlink targets next to the link.
/// Unix-only like every symlink branch upstream (`TORRENT_HAS_SYMLINK`).
#[cfg(unix)]
fn lexically_relative(from: &Path, to: &Path) -> PathBuf {
    // pathdiff-free: both are repo paths; compute `from`-relative
    let from: Vec<_> = from.components().collect();
    let to: Vec<_> = to.components().collect();
    let mut common = 0;
    while common < from.len() && common < to.len() && from[common] == to[common] {
        common += 1;
    }
    let mut out = PathBuf::new();
    for _ in common..from.len() {
        out.push("..");
    }
    for c in &to[common..] {
        out.push(c.as_os_str());
    }
    out
}

/// `aux::create_symlink` — create `link` → `target`; on `EEXIST`
/// verify the existing link already points at `target` (upstream
/// `readlink` compare), else it's an error. POSIX-only upstream
/// (`TORRENT_HAS_SYMLINK`); a no-op elsewhere.
#[cfg(unix)]
pub fn create_symlink(target: &Path, link: &Path) -> StorageResult<()> {
    if let Some(dir) = link.parent()
        && !dir.as_os_str().is_empty()
    {
        fs::create_directories(dir)
            .map_err(|e| StorageError::new(e, FileIndex(-1), Operation::Mkdir))?;
    }
    match std::os::unix::fs::symlink(target, link) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            // it may already be a symlink to the right target —
            // readlink and compare (upstream does the same)
            match std::fs::symlink_metadata(link) {
                Ok(m) if m.file_type().is_symlink() => match std::fs::read_link(link) {
                    Ok(existing) if existing == target => Ok(()),
                    _ => Err(StorageError::new(e, FileIndex(-1), Operation::Symlink)),
                },
                _ => Err(StorageError::new(e, FileIndex(-1), Operation::Symlink)),
            }
        }
        Err(e) => Err(StorageError::new(e, FileIndex(-1), Operation::Symlink)),
    }
}

/// `create_symlink` without `TORRENT_HAS_SYMLINK` — upstream compiles
/// the body out entirely (the call-site is also compiled out).
#[cfg(not(unix))]
pub fn create_symlink(_target: &Path, _link: &Path) -> StorageResult<()> {
    Ok(())
}

/// `errors::mismatching_file_type` — what's on disk doesn't match the
/// torrent's declaration (symlink vs regular file).
fn mismatching_file_type() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "mismatching file type")
}

/// `errors::mismatching_file_size`.
fn mismatching_file_size() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "mismatching file size")
}

/// `aux::move_storage` — move/replace/reset the save directory.
///
/// Returns `(status, new_save_path, error)` mirroring upstream's
/// `(status_t, string)` + `storage_error&` triple. `status` is a
/// `disk_status` bitmask; on `fatal_disk_error`/`file_exist` the path
/// is the *unchanged* save path (upstream commits the new path only
/// on success).
///
/// Behavior matches upstream:
/// - `fail_if_exist` checks every non-absolute file at the target
///   (a *dangling symlink* counts as existing) → `file_exist`;
/// - `reset_save_path`/`reset_save_path_unchecked` short-circuit;
/// - per file: `dont_replace` skips existing targets
///   (`need_full_check`); `rename`, `ENOENT`-tolerated, falls back to
///   `copy_file` (or `create_symlink` for declared+actual links) on
///   any error *except* `invalid_argument`/`permission_denied`;
/// - the part file moves after the files;
/// - on failure everything renamed so far is moved **back**
///   (copied/skipped files are left: their sources never went away)
///   and the verdict is `fatal_disk_error` at the *old* path;
/// - on success copied originals are deleted and the now-empty
///   source-side directories pruned up to `save_path`.
#[allow(clippy::too_many_lines)]
pub fn move_storage(
    fs: &FileStorage,
    renamed: &RenamedFiles,
    save_path: PathBuf,
    destination_save_path: PathBuf,
    mut move_partfile: impl FnMut(&Path) -> io::Result<()>,
    flags: MoveFlags,
) -> (u8, PathBuf, Option<StorageError>) {
    let mut ret = 0u8;
    let new_save_path = fs::complete(&destination_save_path);

    // check to see if any of the files exist
    if flags == MoveFlags::FailIfExist {
        let dir_missing = matches!(
            fs::stat_file(&new_save_path, FileStatusFlags::NONE),
            Err(e) if is_not_found(&e)
        );
        if !dir_missing {
            // the directory exists, check all the files
            for i in fs.file_range() {
                if renamed.file_absolute_path(fs, i) {
                    continue;
                }
                let p = resolve(fs, renamed, i, &new_save_path);
                // a dangling symlink still counts as existing
                match fs::stat_file(&p, FileStatusFlags::DONT_FOLLOW_LINKS) {
                    Err(e) if is_not_found(&e) => {}
                    r => {
                        let err = r
                            .err()
                            .map(|e| StorageError::new(e, i, Operation::FileStat));
                        return (disk_status::FILE_EXIST, save_path, err);
                    }
                }
            }
        }
    }

    match fs::stat_file(&new_save_path, FileStatusFlags::NONE) {
        Err(e) if is_not_found(&e) => {
            if let Err(e2) = fs::create_directories(&new_save_path) {
                return (
                    disk_status::FATAL_DISK_ERROR,
                    save_path,
                    Some(StorageError::new(e2, FileIndex(-1), Operation::Mkdir)),
                );
            }
        }
        Err(e) => {
            return (
                disk_status::FATAL_DISK_ERROR,
                save_path,
                Some(StorageError::new(e, FileIndex(-1), Operation::FileStat)),
            );
        }
        Ok(_) => {}
    }

    match flags {
        MoveFlags::ResetSavePath => {
            return (disk_status::NEED_FULL_CHECK, new_save_path, None);
        }
        MoveFlags::ResetSavePathUnchecked => {
            return (0, new_save_path, None);
        }
        _ => {}
    }

    // indices of all files we ended up copying. These need to be
    // deleted later — rollback leaves them alone (their source is
    // untouched)
    let mut copied_files = vec![false; fs.num_files() as usize];
    // files left untouched: destination already had them
    // (dont_replace) or the source didn't have them. Nothing to undo.
    let mut skipped_files = vec![false; fs.num_files() as usize];

    // track how far we got in case of an error — `usize::MAX` means
    // "the part-file move failed after all files were done" so the
    // rollback sweeps every file
    let mut file_index = FileIndex(0);
    let mut failure: Option<StorageError> = None;
    for i in fs.file_range() {
        if renamed.file_absolute_path(fs, i) {
            continue;
        }
        let old_path = resolve(fs, renamed, i, &save_path);
        let new_path = resolve(fs, renamed, i, &new_save_path);

        // a dangling symlink still counts as existing
        if flags == MoveFlags::DontReplace
            && fs::exists(&new_path, FileStatusFlags::DONT_FOLLOW_LINKS)
        {
            ret |= disk_status::NEED_FULL_CHECK;
            skipped_files[i.get()] = true;
            continue;
        }

        let mut err = move_file(&old_path, &new_path).err();

        // if the source file doesn't exist. That's not a problem
        // we just ignore that file
        if let Some(e) = &err
            && is_not_found(&e.io)
        {
            err = None;
            skipped_files[i.get()] = true;
        } else if let Some(e) = &err
            && !is_invalid_argument(&e.io)
            && !is_permission_denied(&e.io)
        {
            // moving the file failed — on most platforms the error
            // for a cross-volume rename is EXDEV: fall back to
            // copying. `invalid_argument`/`permission_denied` are
            // fatal instead (upstream gates them out).
            let declared_symlink = fs.file_flags(i).contains(FileFlags::SYMLINK);
            let actual_symlink = fs::stat_file(&old_path, FileStatusFlags::DONT_FOLLOW_LINKS)
                .ok()
                .is_some_and(|s| s.mode.contains(crate::fs::FileAttributes::SYMLINK));
            let copy_err = if declared_symlink != actual_symlink {
                // what's on disk does not match what the torrent
                // expects; not safe to assume ownership of this file
                Some(StorageError::new(
                    mismatching_file_type(),
                    i,
                    Operation::FileStat,
                ))
            } else if declared_symlink {
                // `copy_file` would follow the link — move the link
                // itself; the target is relative to the link's parent
                #[cfg(unix)]
                let res = {
                    let link_rel = renamed.file_path(fs, i, &[]);
                    let parent = bytes_to_path(&link_rel);
                    let parent = parent.parent().map_or_else(PathBuf::new, Path::to_path_buf);
                    let target = lexically_relative(&parent, &bytes_to_path(&fs.symlink(i)));
                    create_symlink(&target, &new_path)
                };
                // without symlink support we can't move a link —
                // refuse rather than silently dropping the file
                #[cfg(not(unix))]
                let res: crate::storage_error::StorageResult<()> = Err(StorageError::new(
                    mismatching_file_type(),
                    i,
                    Operation::Symlink,
                ));
                res.err()
            } else {
                match fs::copy_file(&old_path, &new_path) {
                    Ok(()) => None,
                    Err(e2) => Some(StorageError::new(e2, i, Operation::FileCopy)),
                }
            };
            match copy_err {
                None => {
                    copied_files[i.get()] = true;
                    err = None;
                }
                Some(e2) => err = Some(e2),
            }
        }

        if let Some(e) = err {
            file_index = i;
            failure = Some(StorageError::new(e.io, i, e.operation));
            break;
        }
    }

    if failure.is_none() {
        match move_partfile(&new_save_path) {
            Ok(()) => {}
            Err(e) => {
                failure = Some(StorageError::new(e, FileIndex(-1), Operation::PartfileMove));
                // rollback sweeps every file
                file_index = FileIndex(fs.num_files());
            }
        }
    }

    if let Some(err) = failure {
        // rollback — move every file we renamed back to the source
        // (copied files kept their source; skipped files never moved)
        let mut i = file_index;
        loop {
            i = FileIndex(i.0 - 1);
            if i.0 < 0 {
                break;
            }
            if renamed.file_absolute_path(fs, i) || copied_files[i.get()] || skipped_files[i.get()]
            {
                continue;
            }
            let old_path = resolve(fs, renamed, i, &save_path);
            let new_path = resolve(fs, renamed, i, &new_save_path);
            // ignore errors when rolling back
            let _ = move_file(&new_path, &old_path);
        }
        return (disk_status::FATAL_DISK_ERROR, save_path, Some(err));
    }

    // delete the originals of files that had to be *copied* — rename
    // already removed them
    let mut subdirs: BTreeSet<PathBuf> = BTreeSet::new();
    for i in fs.file_range() {
        if renamed.file_absolute_path(fs, i) {
            continue;
        }
        let rel = renamed.file_path(fs, i, &[]);
        let rel_p = bytes_to_path(&rel);
        if let Some(parent) = rel_p.parent()
            && !parent.as_os_str().is_empty()
        {
            subdirs.insert(parent.to_path_buf());
        }
        if !copied_files[i.get()] {
            continue;
        }
        // we may still have some files in old save_path
        // (dont_replace left them) — ignore errors when removing
        let _ = fs::remove(&resolve(fs, renamed, i, &save_path));
    }

    // remove now-empty directories under the old save_path, walking
    // up until the save_path itself
    for s in &subdirs {
        let mut subdir = save_path.join(s);
        while subdir != save_path && fs::remove(&subdir).is_ok() {
            let Some(parent) = subdir.parent() else {
                break;
            };
            subdir = parent.to_path_buf();
        }
    }

    (ret, new_save_path, None)
}

/// `aux::delete_one_file` — remove, ignoring `ENOENT`.
fn delete_one_file(p: &Path) -> Option<io::Error> {
    match fs::remove(p) {
        Err(e) if is_not_found(&e) => None,
        Err(e) => Some(e),
        Ok(()) => None,
    }
}

/// `aux::delete_files` — delete torrent files + optionally the part
/// file; then remove now-empty parent directories (deepest first).
/// Upstream keeps going past individual file errors — a partially
/// deleted torrent still frees as much as it can; the *first* error
/// (per phase) is reported.
pub fn delete_files(
    fs: &FileStorage,
    renamed: &RenamedFiles,
    save_path: &Path,
    part_file: &Path,
    options: u8,
) -> StorageResult<()> {
    let mut first_err: Option<StorageError> = None;
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
            if let Some(e) = delete_one_file(&p)
                && first_err.is_none()
            {
                first_err = Some(StorageError::new(e, i, Operation::FileRemove));
            }
        }
        // deepest first — depth order beats upstream's
        // lexicographic-reverse approximation for empty-dir removal
        let mut dirs: Vec<PathBuf> = directories.into_iter().collect();
        dirs.sort_by_key(|d| std::cmp::Reverse(d.components().count()));
        for d in dirs {
            if let Some(e) = delete_one_file(&d)
                && first_err.is_none()
            {
                first_err = Some(StorageError::new(e, FileIndex(-1), Operation::FileRemove));
            }
        }
    }

    // when we're deleting "files", we also delete the part file
    if options & (remove_flags::DELETE_PARTFILE | remove_flags::DELETE_FILES) != 0
        && let Some(e) = delete_one_file(part_file)
        && first_err.is_none()
    {
        first_err = Some(StorageError::new(e, FileIndex(-1), Operation::FileRemove));
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// `aux::rename_file` — rename the on-disk file when it exists;
/// otherwise just record the rename. Refuses to overwrite.
///
/// Upstream details preserved:
/// - `ENOENT` **and** `filename_too_long` on the source take the
///   record-only shortcut (the destination dir may not exist yet);
/// - an existing destination (dangling symlink included) refuses;
/// - a failed rename falls back to copy+remove for anything *other*
///   than `invalid_argument`/`permission_denied` (e.g. `EXDEV`),
///   with the declared-vs-actual symlink check first.
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

    // also tells us whether old_name is itself a symlink — needed if
    // the cross-filesystem fallback has to recreate it instead of
    // copying its content
    let old_stat = match fs::stat_file(&old_path, FileStatusFlags::DONT_FOLLOW_LINKS) {
        Err(e) if is_not_found(&e) || is_filename_too_long(&e) => {
            // `no_such_file_or_directory || filename_too_long` — if
            // the old file doesn't exist, just succeed and change the
            // filename that will be created
            // if the old file doesn't exist, just succeed and change
            // the filename that will be created
            renamed.rename_file(fs, index, new_filename);
            return Ok(());
        }
        Err(e) => return Err(StorageError::new(e, index, Operation::FileStat)),
        Ok(s) => s,
    };

    let new_name_path = bytes_to_path(new_filename);
    let new_path = if new_name_path.is_absolute() {
        new_name_path
    } else {
        save_path.join(&new_name_path)
    };
    let new_dir = new_path
        .parent()
        .map_or_else(PathBuf::new, Path::to_path_buf);

    // We don't want to overwrite an existing file — a dangling
    // symlink at the destination counts
    if fs::exists(&new_path, FileStatusFlags::DONT_FOLLOW_LINKS) {
        return Err(StorageError::new(
            io::Error::from(io::ErrorKind::AlreadyExists),
            index,
            Operation::FileRename,
        ));
    }

    fs::create_directories(&new_dir).map_err(|e| StorageError::new(e, index, Operation::Mkdir))?;

    let declared_symlink = fs.file_flags(index).contains(FileFlags::SYMLINK);
    let old_is_symlink = old_stat.mode.contains(crate::fs::FileAttributes::SYMLINK);

    match fs::rename(&old_path, &new_path) {
        // if old_name doesn't exist, that's not an error here. Once
        // we start writing to the file, it will be written to the new
        // filename
        Err(e) if is_not_found(&e) => {}
        Err(e) if is_hard_rename_error(&e) => {
            return Err(StorageError::new(e, index, Operation::FileRename));
        }
        Err(_) => {
            // the rename failed for another reason — copy covers
            // e.g. EXDEV
            if old_is_symlink != declared_symlink {
                return Err(StorageError::new(
                    mismatching_file_type(),
                    index,
                    Operation::FileStat,
                ));
            }
            if old_is_symlink {
                // `new_filename` may sit at an unrelated depth — the
                // target is anchored to save_path (upstream comment)
                #[cfg(unix)]
                let res = {
                    let target = lexically_relative(
                        &new_dir,
                        &save_path.join(bytes_to_path(&fs.symlink(index))),
                    );
                    create_symlink(&target, &new_path)
                };
                #[cfg(not(unix))]
                let res = Err(StorageError::new(
                    mismatching_file_type(),
                    index,
                    Operation::Symlink,
                ));
                res?;
            } else {
                fs::copy_file(&old_path, &new_path)
                    .map_err(|e| StorageError::new(e, index, Operation::FileCopy))?;
            }
            // ignore errors when removing the copy source
            let _ = fs::remove(&old_path);
        }
        Ok(()) => {}
    }

    renamed.rename_file(fs, index, new_filename);
    Ok(())
}

/// `aux::has_any_file` — true when any file has a non-zero size on
/// disk. Upstream does NOT pre-filter pad/empty files (`sz > 0` does
/// the filtering) and reports the first hard `stat` error with the
/// cache cleared.
pub fn has_any_file(
    fs: &FileStorage,
    renamed: &RenamedFiles,
    save_path: &Path,
    sc: &StatCache,
) -> StorageResult<bool> {
    for i in fs.file_range() {
        match stat_filesize(sc, i, fs, renamed, save_path) {
            Ok(sz) => {
                if sz > 0 {
                    return Ok(true);
                }
            }
            Err(e) if is_not_found(&e) => {
                // some files not existing is expected and not an error
            }
            Err(e) => {
                sc.clear();
                return Err(StorageError::new(e, i, Operation::FileStat));
            }
        }
    }
    Ok(false)
}

/// `stat_cache::get_filesize` helper shared by `has_any_file` and
/// `verify_resume_data`: upstream pretends symlink-flagged files
/// don't exist ("they're zero size anyway") before hitting the cache.
fn stat_filesize(
    stat: &StatCache,
    i: FileIndex,
    fs: &FileStorage,
    renamed: &RenamedFiles,
    save_path: &Path,
) -> io::Result<i64> {
    if fs.file_flags(i).contains(FileFlags::SYMLINK) {
        return Err(io::Error::from(io::ErrorKind::NotFound));
    }
    stat.get_filesize_path(i, &resolve(fs, renamed, i, save_path))
}

/// `aux::verify_resume_data`'s private `get_filesize` — ENOENT and
/// symlink files count as `mismatching_file_size`, other errors are
/// passed through; both record `file_stat` upstream.
fn get_filesize(
    stat: &StatCache,
    i: FileIndex,
    fs: &FileStorage,
    renamed: &RenamedFiles,
    save_path: &Path,
) -> Result<i64, StorageError> {
    match stat_filesize(stat, i, fs, renamed, save_path) {
        Ok(sz) => Ok(sz),
        Err(e) if is_not_found(&e) => Err(StorageError::new(
            mismatching_file_size(),
            i,
            Operation::FileStat,
        )),
        Err(e) => Err(StorageError::new(e, i, Operation::FileStat)),
    }
}

/// `aux::verify_resume_data` — validate the fastresume record against
/// the filesystem. Returns `(verify_success, error)` — upstream's
/// `bool` + `storage_error&` pair: **`false` does not imply an
/// error**, and an error may accompany `false` (the caller maps it to
/// `need_full_check` + the alert detail, not a fatal verdict).
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
) -> (bool, Option<StorageError>) {
    let mut added_files = false;
    if !links.is_empty() {
        debug_assert_eq!(links.len(), fs.num_files() as usize);
        for idx in fs.file_range() {
            let Some(s) = links.get(idx.get()) else {
                break;
            };
            if s.as_os_str().is_empty() {
                continue;
            }
            let file_path = resolve(fs, renamed, idx, save_path);
            if let Err(e) = fs::hard_link(s, &file_path) {
                if is_not_found(&e) {
                    // we create directories lazily — it may not exist
                    // yet; create them and try again
                    if let Some(parent) = file_path.parent()
                        && let Err(e2) = fs::create_directories(parent)
                    {
                        return (false, Some(StorageError::new(e2, idx, Operation::Mkdir)));
                    }
                    match fs::hard_link(s, &file_path) {
                        // if the file already exists, that's not an
                        // error
                        Err(e2) if e2.kind() == io::ErrorKind::AlreadyExists => continue,
                        Err(e2) => {
                            return (
                                false,
                                Some(StorageError::new(e2, idx, Operation::FileHardLink)),
                            );
                        }
                        Ok(()) => {}
                    }
                } else if e.kind() == io::ErrorKind::AlreadyExists {
                    continue;
                } else {
                    return (
                        false,
                        Some(StorageError::new(e, idx, Operation::FileHardLink)),
                    );
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
            // files with priority zero may not have been saved to disk
            // at their expected location, but are likely in a
            // partfile. Just exempt them from checking.
            if i.get() < file_priority.len()
                && file_priority[i.get()] == DownloadPriority::DONT_DOWNLOAD
                && !seed_mode
            {
                continue;
            }
            let size = match get_filesize(stat, i, fs, renamed, save_path) {
                Ok(s) => s,
                Err(e) => return (false, Some(e)),
            };
            if size < fs.file_size(i) {
                return (
                    false,
                    Some(StorageError::new(
                        mismatching_file_size(),
                        i,
                        Operation::CheckResume,
                    )),
                );
            }
        }
        return (true, None);
    }

    // always trigger a full recheck when we pull in files from other
    // torrents, via hard links
    if added_files {
        return (false, None);
    }

    // parse have bitmask. Verify that files touched by completed
    // pieces actually exist. A completed piece may span multiple
    // files.
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
        if let Err(e) = get_filesize(stat, i, fs, renamed, save_path) {
            return (false, Some(e));
        }
    }
    (true, None)
}

/// `aux::initialize_storage` — create zero-sized files (and symlinks)
/// that don't exist yet; never truncates. `oversized_file` is invoked
/// for *every* file larger than its declared size (not just
/// priority-0 — upstream reports them all).
#[allow(clippy::too_many_arguments)]
pub fn initialize_storage(
    fs: &FileStorage,
    renamed: &RenamedFiles,
    save_path: &Path,
    sc: &StatCache,
    file_priority: &[DownloadPriority],
    mut create_file: impl FnMut(FileIndex) -> StorageResult<()>,
    mut oversized_file: impl FnMut(FileIndex, i64),
) -> StorageResult<()> {
    // create zero-sized files
    for i in fs.file_range() {
        // ignore files that have priority 0
        if i.get() < file_priority.len()
            && file_priority[i.get()] == DownloadPriority::DONT_DOWNLOAD
        {
            continue;
        }
        // ignore pad files
        if fs.pad_file_at(i) {
            continue;
        }
        // this is just to see if the file exists
        let stat_res = sc.get_filesize_path(i, &resolve(fs, renamed, i, save_path));
        if let Err(e) = &stat_res
            && !is_not_found(e)
        {
            return Err(StorageError::new(
                io::Error::new(e.kind(), e.to_string()),
                i,
                Operation::FileStat,
            ));
        }
        let fs_file_size = fs.file_size(i);
        if let Ok(sz) = stat_res
            && sz > fs_file_size
        {
            // this file is oversized, alert the client
            oversized_file(i, sz);
        }

        // if the file is empty and doesn't already exist, create it
        // deliberately don't truncate files that already exist
        if fs_file_size == 0 {
            // `TORRENT_HAS_SYMLINK` — the whole branch compiles out
            // upstream where symlinks aren't supported; the file then
            // falls through to `create_file` like any empty file.
            #[cfg(unix)]
            let handled = if fs.file_flags(i).contains(FileFlags::SYMLINK) {
                let path = resolve(fs, renamed, i, save_path);
                // the symlink target is relative to the link itself
                let link_rel = bytes_to_path(&renamed.file_path(fs, i, &[]));
                let parent = link_rel
                    .parent()
                    .map_or_else(PathBuf::new, Path::to_path_buf);
                let target = lexically_relative(&parent, &bytes_to_path(&fs.symlink(i)));
                create_symlink(&target, &path)
                    .map_err(|e| StorageError::new(e.io, i, e.operation))?;
                true
            } else {
                false
            };
            #[cfg(not(unix))]
            let handled = false;
            if !handled && matches!(stat_res, Err(ref e) if is_not_found(e)) {
                // just creating the file is enough to make it
                // zero-sized
                create_file(i)?;
            }
        }
    }
    Ok(())
}

/// Port of `truncate.cpp`'s `truncate_files` — the public helper a
/// client runs after an `oversized_file_alert`: every non-pad file
/// that exists on disk and is longer than its declared size is
/// truncated back to it (`file_open`/`file_stat`/`file_truncate`
/// ops on error, `ENOENT` skipped).
pub fn truncate_files(fs: &FileStorage, save_path: &Path) -> StorageResult<()> {
    for i in fs.file_range() {
        if fs.pad_file_at(i) {
            continue;
        }
        let rel = fs.file_path(i, &[]);
        let path = save_path.join(bytes_to_path(&rel));
        let file = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
        {
            Ok(f) => f,
            Err(e) if is_not_found(&e) => continue,
            Err(e) => return Err(StorageError::new(e, i, Operation::FileOpen)),
        };
        let len = file
            .metadata()
            .map_err(|e| StorageError::new(e, i, Operation::FileStat))?
            .len();
        if (len as i64) < fs.file_size(i) {
            continue;
        }
        file.set_len(u64::try_from(fs.file_size(i)).unwrap_or(0))
            .map_err(|e| StorageError::new(e, i, Operation::FileTruncate))?;
    }
    Ok(())
}
