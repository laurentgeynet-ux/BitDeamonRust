//! Port of `aux_/readwrite.hpp` — the piece→file walk shared by
//! `pread_storage::read`/`write`/`hash`.
//!
//! `readwrite` splits a piece-range buffer into per-file slices and
//! feeds them to `op`. `op` returns bytes transferred or a
//! `StorageError`; a short transfer with bytes remaining is surfaced
//! as `UnexpectedEof` (upstream `asio::error::eof` semantics).

use bitdaemon_common::types::FileIndex;
use bitdaemon_common::types::PieceIndex;
use bitdaemon_core::file_storage::FileStorage;

use crate::storage_error::{Operation, StorageError};

/// `readwrite_impl` — walk `buf` across the files covering
/// `[piece*piece_length + offset, +len)`.
///
/// `op(file_index, file_offset, slice)` → `Result<usize, StorageError>`
/// (bytes consumed; errors are annotated with the file index).
pub fn readwrite_buf<Op>(
    files: &FileStorage,
    buf: &[u8],
    piece: PieceIndex,
    offset: i64,
    mut op: Op,
) -> Result<i64, StorageError>
where
    Op: FnMut(FileIndex, i64, &[u8]) -> Result<usize, StorageError>,
{
    debug_assert!(piece.0 >= 0);
    debug_assert!(piece < files.end_piece());
    debug_assert!(offset >= 0);
    debug_assert!(!buf.is_empty());

    let torrent_offset = i64::from(piece.0) * i64::from(files.piece_length()) + offset;
    let mut file_index = files.file_index_at_offset(torrent_offset);
    let mut file_offset = torrent_offset - files.file_offset(file_index);

    let mut ret = 0i64;
    let mut remaining = buf.len();

    while remaining > 0 {
        let mut file_bytes_left = i64::try_from(remaining).unwrap_or(i64::MAX);
        if file_offset + file_bytes_left > files.file_size(file_index) {
            file_bytes_left = std::cmp::max(files.file_size(file_index) - file_offset, 0);
        }

        // skip empty files
        if file_bytes_left == 0 {
            loop {
                file_index = FileIndex(file_index.0 + 1);
                file_offset = 0;
                if file_index >= files.end_file() {
                    return Ok(ret);
                }
                if files.file_size(file_index) > 0 {
                    break;
                }
            }
            file_bytes_left = i64::try_from(remaining).unwrap_or(i64::MAX);
            if file_offset + file_bytes_left > files.file_size(file_index) {
                file_bytes_left = std::cmp::max(files.file_size(file_index) - file_offset, 0);
            }
            debug_assert!(file_bytes_left > 0);
        }

        let slice_len = usize::try_from(file_bytes_left).unwrap_or(0);
        let consumed = buf.len() - remaining;
        let bytes_transferred = op(
            file_index,
            file_offset,
            &buf[consumed..consumed + slice_len],
        )
        .map_err(|mut e| {
            e.file = file_index;
            e
        })?;
        // op must not overrun the slice
        debug_assert!(i64::try_from(bytes_transferred).unwrap_or(0) <= file_bytes_left);

        let n = i64::try_from(bytes_transferred).unwrap_or(0);
        remaining -= usize::try_from(n).unwrap_or(0);
        file_offset += n;
        ret += n;

        // a 0-byte transfer means EOF — surface it as the eof error
        if bytes_transferred == 0 && file_bytes_left > 0 {
            return Err(StorageError::new(
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof"),
                file_index,
                Operation::FileRead,
            ));
        }
    }
    Ok(ret)
}

/// Mutable-buffer variant for reads — `op` receives `&mut [u8]` carved
/// out of `buf` (no aliasing: each slice is handed out once).
pub fn readwrite_mut<Op>(
    files: &FileStorage,
    buf: &mut [u8],
    piece: PieceIndex,
    offset: i64,
    mut op: Op,
) -> Result<i64, StorageError>
where
    Op: FnMut(FileIndex, i64, &mut [u8]) -> Result<usize, StorageError>,
{
    debug_assert!(piece.0 >= 0);
    debug_assert!(piece < files.end_piece());
    debug_assert!(offset >= 0);
    debug_assert!(!buf.is_empty());

    let torrent_offset = i64::from(piece.0) * i64::from(files.piece_length()) + offset;
    let mut file_index = files.file_index_at_offset(torrent_offset);
    let mut file_offset = torrent_offset - files.file_offset(file_index);

    let mut ret = 0i64;
    let mut pos = 0usize;

    while pos < buf.len() {
        let mut file_bytes_left = i64::try_from(buf.len() - pos).unwrap_or(i64::MAX);
        if file_offset + file_bytes_left > files.file_size(file_index) {
            file_bytes_left = std::cmp::max(files.file_size(file_index) - file_offset, 0);
        }
        if file_bytes_left == 0 {
            loop {
                file_index = FileIndex(file_index.0 + 1);
                file_offset = 0;
                if file_index >= files.end_file() {
                    return Ok(ret);
                }
                if files.file_size(file_index) > 0 {
                    break;
                }
            }
            file_bytes_left = i64::try_from(buf.len() - pos).unwrap_or(i64::MAX);
            if file_offset + file_bytes_left > files.file_size(file_index) {
                file_bytes_left = std::cmp::max(files.file_size(file_index) - file_offset, 0);
            }
        }
        let slice_len = usize::try_from(file_bytes_left).unwrap_or(0);
        let slice = &mut buf[pos..pos + slice_len];
        let n = op(file_index, file_offset, slice).map_err(|mut e| {
            e.file = file_index;
            e
        })?;
        debug_assert!(n <= slice_len);
        pos += n;
        file_offset += i64::try_from(n).unwrap_or(0);
        ret += i64::try_from(n).unwrap_or(0);
        if n == 0 && file_bytes_left > 0 {
            return Err(StorageError::new(
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof"),
                file_index,
                Operation::FileRead,
            ));
        }
    }
    Ok(ret)
}
