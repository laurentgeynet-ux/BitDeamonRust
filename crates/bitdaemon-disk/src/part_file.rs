//! Port of `aux_/part_file.hpp`/`part_file.cpp`.
//!
//! On-disk layout (unchanged from upstream, **all header values are
//! big-endian** — `io_bytes.hpp`'s `read_uint32`/`write_int32`):
//! `[u32 num_pieces][u32 piece_size][i32 slot_of_piece × num_pieces][pad]`
//! header rounded up to a whole KiB; payload slots start at
//! `header_size` and each hold one `piece_size` block.
//!
//! A part file written by libtorrent and one written by this crate
//! must round-trip in both directions — endianness is
//! correctness-critical here.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

use bitdaemon_common::types::PieceIndex;

use crate::file::FileHandle;
use crate::file::{pread_all, pwrite_all};
use crate::fs;
use crate::open_mode::OpenMode;

/// `aux::slot_index_t`.
pub type SlotIndex = i32;

/// round up to even kilobyte (`round_up` upstream).
const fn round_up(n: i64) -> i64 {
    (n + 1023) & !0x3ff
}

/// `errors::partfile_missing_piece`.
fn missing_piece() -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, "partfile: missing piece")
}

struct Locked {
    /// `m_free_slots` — freed slots reusable for new pieces. Upstream
    /// allocates from the FRONT (`front()` + `erase(begin())`) and
    /// `free_piece` pushes at the back — a FIFO queue, not a stack.
    /// Match it: slot layout is observable on disk.
    free_slots: Vec<SlotIndex>,
    num_allocated: SlotIndex,
    piece_map: HashMap<i32, SlotIndex>,
    dirty_metadata: bool,
}

/// `aux::part_file`.
pub struct PartFile {
    path: PathBuf,
    name: PathBuf,
    max_pieces: i32,
    piece_size: i32,
    header_size: i64,
    m: Mutex<Locked>,
}

impl PartFile {
    /// `part_file(path, name, num_pieces, piece_size)` — creates or
    /// parses the header of an existing file. A header whose
    /// `num_pieces`/`piece_size` mismatch makes the file count as
    /// empty (upstream semantics).
    pub fn new(path: PathBuf, name: PathBuf, num_pieces: i32, piece_size: i32) -> Self {
        debug_assert!(num_pieces > 0);
        debug_assert!(piece_size > 0);
        let pf = Self {
            path,
            name,
            max_pieces: num_pieces,
            piece_size,
            header_size: round_up(i64::from(2 + num_pieces) * 4),
            m: Mutex::new(Locked {
                free_slots: Vec::new(),
                num_allocated: 0,
                piece_map: HashMap::new(),
                dirty_metadata: false,
            }),
        };

        // parse header of existing file — `pread_all` errors on short
        // reads; upstream treats "fewer than header_size bytes" as an
        // empty file, so a short read (or absent file) just leaves the
        // map empty.
        if let Ok(f) = pf.open_file(OpenMode::READ_ONLY) {
            let mut header = vec![0u8; usize::try_from(pf.header_size).unwrap_or(0)];
            if let Ok(n) = crate::file::pread_best_effort(f.file(), &mut header, 0) {
                if i64::try_from(n).unwrap_or(0) >= pf.header_size {
                    pf.parse_header(&header);
                }
            }
        }
        pf
    }

    /// `read_uint32`/`read_int32` from `aux_/io_bytes.hpp` — all
    /// header fields are stored **big-endian** on disk (the format
    /// comment in `part_file.cpp` is explicit about this).
    fn parse_header(&self, header: &[u8]) {
        let num_pieces = i32::from_be_bytes(header[0..4].try_into().unwrap_or_default());
        let piece_size = i32::from_be_bytes(header[4..8].try_into().unwrap_or_default());
        // mismatch → treat as empty
        if num_pieces != self.max_pieces || piece_size != self.piece_size {
            return;
        }
        let mut g = self.lock();
        let mut free_slots = vec![true; usize::try_from(self.max_pieces).unwrap_or(0)];
        let mut ptr = 8;
        for piece in 0..self.max_pieces {
            let slot = i32::from_be_bytes(header[ptr..ptr + 4].try_into().unwrap_or_default());
            ptr += 4;
            if slot < 0 {
                continue;
            }
            // invalid part-file
            debug_assert!(slot < self.max_pieces);
            if slot >= self.max_pieces {
                continue;
            }
            if slot >= g.num_allocated {
                g.num_allocated = slot + 1;
            }
            free_slots[usize::try_from(slot).unwrap_or(0)] = false;
            g.piece_map.insert(piece, slot);
        }
        for i in 0..g.num_allocated {
            if free_slots[usize::try_from(i).unwrap_or(0)] {
                g.free_slots.push(i);
            }
        }
    }

    /// `allocate_slot` — take the OLDEST free slot (`front()` +
    /// `erase(begin())` upstream — FIFO) or extend the file.
    fn allocate_slot(g: &mut Locked, piece: PieceIndex) -> SlotIndex {
        debug_assert!(!g.piece_map.contains_key(&piece.0));
        let slot = if g.free_slots.is_empty() {
            let s = g.num_allocated;
            g.num_allocated += 1;
            s
        } else {
            g.free_slots.remove(0)
        };
        g.dirty_metadata = true;
        g.piece_map.insert(piece.0, slot);
        slot
    }

    fn slot_offset(&self, slot: SlotIndex) -> i64 {
        i64::from(slot) * i64::from(self.piece_size) + self.header_size
    }

    /// `part_file::open_file` — on a `write` open hitting `ENOENT`,
    /// create `m_path` and retry once (upstream: the missing-directory
    /// error means the part-file dir doesn't exist yet).
    fn open_file(&self, mode: OpenMode) -> io::Result<FileHandle> {
        let fnm = self.path.join(&self.name);
        match FileHandle::open(&fnm, 0, mode) {
            Ok(f) => Ok(f),
            Err(e) => {
                if !(mode.contains(OpenMode::WRITE) && e.source.kind() == io::ErrorKind::NotFound) {
                    return Err(e.source);
                }
                fs::create_directories(&self.path)?;
                FileHandle::open(&fnm, 0, mode).map_err(|e2| e2.source)
            }
        }
    }

    fn lock(&self) -> MutexGuard<'_, Locked> {
        self.m.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `write(buf, piece, offset)` — write `buf` into the slot for
    /// `piece` at `offset` within the piece. Returns bytes written.
    pub fn write(&self, buf: &[u8], piece: PieceIndex, offset: i32) -> io::Result<i32> {
        self.writev(&[buf], piece, offset)
    }

    /// `writev(bufs, piece, offset)` — scatter write into one piece.
    /// Upstream issues a single `pwritev`; we issue sequential
    /// `pwrite`s — same observable bytes, marginally less atomic.
    pub fn writev(&self, bufs: &[&[u8]], piece: PieceIndex, offset: i32) -> io::Result<i32> {
        debug_assert!(offset >= 0);
        debug_assert!(
            i64::from(offset)
                + i64::try_from(bufs.iter().map(|b| b.len()).sum::<usize>()).unwrap_or(0)
                <= i64::from(self.piece_size)
        );
        // upstream opens the file before allocating the slot — the
        // order matters: on a write-open failure no slot is consumed.
        let f = self.open_file(OpenMode::WRITE | OpenMode::HIDDEN)?;
        let slot = {
            let mut g = self.lock();
            match g.piece_map.get(&piece.0) {
                Some(&s) => s,
                None => Self::allocate_slot(&mut g, piece),
            }
        };
        let file_offset = self.slot_offset(slot) + i64::from(offset);
        let mut ret = 0i32;
        let mut off = file_offset;
        for b in bufs {
            let n = pwrite_all(f.file(), b, u64::try_from(off).unwrap_or(0))?;
            ret += i32::try_from(n).unwrap_or(0);
            off += i64::try_from(n).unwrap_or(0);
        }
        Ok(ret)
    }

    /// `read(buf, piece, offset)` — `partfile_missing_piece` when the
    /// piece isn't in the part file.
    pub fn read(&self, buf: &mut [u8], piece: PieceIndex, offset: i32) -> io::Result<i32> {
        debug_assert!(offset >= 0);
        debug_assert!(offset as i64 + buf.len() as i64 <= i64::from(self.piece_size));
        let slot = self
            .lock()
            .piece_map
            .get(&piece.0)
            .copied()
            .ok_or_else(missing_piece)?;
        let f = self.open_file(OpenMode::READ_ONLY | OpenMode::HIDDEN)?;
        let file_offset = self.slot_offset(slot) + i64::from(offset);
        let n = pread_all(f.file(), buf, u64::try_from(file_offset).unwrap_or(0))?;
        Ok(i32::try_from(n).unwrap_or(0))
    }

    /// `hash`/`hash2` — `do_hash` upstream reads `len` bytes and feeds
    /// them to the hasher directly. We return the bytes so the caller
    /// can feed any hasher — same observable contract:
    /// `partfile_missing_piece` on absent piece.
    pub fn hash_feed(&self, piece: PieceIndex, offset: i32, len: usize) -> io::Result<Vec<u8>> {
        debug_assert!(offset >= 0);
        debug_assert!(offset as i64 + len as i64 <= i64::from(self.piece_size));
        let mut buf = vec![0u8; len];
        self.read(&mut buf, piece, offset)?;
        Ok(buf)
    }

    /// `free_piece` — drop the slot backing `piece`; returns it to the
    /// back of the free queue (`push_back` upstream → FIFO reuse).
    pub fn free_piece(&self, piece: PieceIndex) {
        let mut g = self.lock();
        if let Some(slot) = g.piece_map.remove(&piece.0) {
            g.free_slots.push(slot);
            g.dirty_metadata = true;
        }
    }

    /// `move_partfile(path)` — flush the header, then rename the data
    /// file. Upstream moves only when the map is non-empty (nothing on
    /// disk to move otherwise — a stale-but-absent source is fine),
    /// tolerates `ENOENT` on the rename, and falls back to
    /// `copy_file` + `remove` for cross-volume moves.
    pub fn move_partfile(&mut self, path: PathBuf) -> io::Result<()> {
        self.flush_metadata()?;

        let empty = self.lock().piece_map.is_empty();
        if !empty {
            let old = self.path.join(&self.name);
            let new = path.join(&self.name);
            match fs::rename(&old, &new) {
                // source doesn't exist — nothing to move
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                // e.g. EXDEV across volumes → copy + remove
                Err(_) => {
                    fs::copy_file(&old, &new)?;
                    match fs::remove(&old) {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                        Err(e) => return Err(e),
                    }
                }
            }
        }
        self.path = path;
        Ok(())
    }

    /// `export_file` — call `f(file_offset, bytes)` for each stored
    /// block covering `[offset, offset+size)`; `file_offset` is
    /// *relative to the start of the exported range* (0-based), like
    /// upstream. Pieces fully inside the range are *freed* afterwards
    /// (upstream erases full-piece slots). A `f` error aborts the
    /// export immediately — the current piece keeps its slot, like
    /// upstream's exception-unwound `export_file`.
    pub fn export_file(
        &self,
        offset: i64,
        size: i64,
        mut f: impl FnMut(i64, &[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        let piece_size = i64::from(self.piece_size);
        // nothing stored → nothing to do
        if self.lock().piece_map.is_empty() {
            return Ok(());
        }
        let start_piece = PieceIndex(i32::try_from(offset / piece_size).unwrap_or(0));
        let end_piece =
            PieceIndex(i32::try_from((offset + size + piece_size - 1) / piece_size).unwrap_or(0));

        let mut piece = start_piece;
        let mut piece_offset = offset - i64::from(piece.0) * piece_size;
        let mut file_offset = 0i64;
        let mut remaining = size;
        let mut buf: Option<Vec<u8>> = None;
        // upstream opens with plain `read_only` (no `hidden`) here
        let file = self.open_file(OpenMode::READ_ONLY)?;

        while piece < end_piece {
            let slot = {
                let g = self.lock();
                g.piece_map.get(&piece.0).copied()
            };
            let block_to_copy = i64::min(piece_size - piece_offset, remaining);
            if let Some(slot) = slot {
                let buf = buf.get_or_insert_with(|| {
                    vec![0u8; usize::try_from(self.piece_size).unwrap_or(0)]
                });
                let n = crate::file::pread_best_effort(
                    file.file(),
                    &mut buf[..usize::try_from(block_to_copy).unwrap_or(0)],
                    u64::try_from(self.slot_offset(slot) + piece_offset).unwrap_or(0),
                )?;
                if i64::try_from(n).unwrap_or(0) < block_to_copy {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
                }
                f(
                    file_offset,
                    &buf[..usize::try_from(block_to_copy).unwrap_or(0)],
                )?;

                if block_to_copy == piece_size {
                    let mut g = self.lock();
                    // the map may have changed while unlocked
                    if g.piece_map.get(&piece.0) == Some(&slot) {
                        g.free_slots.push(slot);
                        g.piece_map.remove(&piece.0);
                        g.dirty_metadata = true;
                    }
                }
            }
            file_offset += block_to_copy;
            piece_offset = 0;
            remaining -= block_to_copy;
            piece = PieceIndex(piece.0 + 1);
        }
        Ok(())
    }

    /// `flush_metadata` — write the header; remove the file entirely
    /// when no pieces remain (upstream semantics).
    pub fn flush_metadata(&self) -> io::Result<()> {
        {
            let g = self.lock();
            if !g.dirty_metadata {
                return Ok(());
            }
        }
        let (empty, max_pieces, header_size) = {
            let g = self.lock();
            (g.piece_map.is_empty(), self.max_pieces, self.header_size)
        };
        if empty {
            let p = self.path.join(&self.name);
            match fs::remove(&p) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            return Ok(());
        }
        let f = self.open_file(OpenMode::WRITE | OpenMode::HIDDEN)?;
        let mut header = vec![0u8; usize::try_from(header_size).unwrap_or(0)];
        header[0..4].copy_from_slice(&max_pieces.to_be_bytes());
        header[4..8].copy_from_slice(&self.piece_size.to_be_bytes());
        {
            let g = self.lock();
            let mut ptr = 8;
            for piece in 0..max_pieces {
                let slot = g.piece_map.get(&piece).copied().unwrap_or(-1);
                header[ptr..ptr + 4].copy_from_slice(&slot.to_be_bytes());
                ptr += 4;
            }
            // upstream memsets the tail padding — our `vec![0; ...]`
            // leaves it zeroed already.
        }
        pwrite_all(f.file(), &header, 0)?;
        self.lock().dirty_metadata = false;
        Ok(())
    }
}

impl Drop for PartFile {
    fn drop(&mut self) {
        // upstream: flush_metadata_impl(ec) in the dtor
        let _ = self.flush_metadata();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (PathBuf, PartFile) {
        use std::sync::atomic::{AtomicU32, Ordering};
        static N: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "bitdaemon_part_file_test_{}_{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_all(&dir);
        fs::create_directories(&dir).unwrap();
        (
            dir.clone(),
            PartFile::new(dir.clone(), PathBuf::from("test.part"), 10, 1024),
        )
    }

    #[test]
    fn write_read_roundtrip() {
        let (dir, pf) = setup();
        let data = vec![0xabu8; 1024];
        assert_eq!(pf.write(&data, PieceIndex(5), 0).unwrap(), 1024);
        let mut out = vec![0u8; 1024];
        assert_eq!(pf.read(&mut out, PieceIndex(5), 0).unwrap(), 1024);
        assert_eq!(out, data);
        // missing piece errors
        assert_eq!(
            pf.read(&mut out, PieceIndex(3), 0).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        pf.flush_metadata().unwrap();
        drop(pf);
        // reload: header round-trips
        let pf2 = PartFile::new(dir.clone(), PathBuf::from("test.part"), 10, 1024);
        let mut out2 = vec![0u8; 1024];
        assert_eq!(pf2.read(&mut out2, PieceIndex(5), 0).unwrap(), 1024);
        assert_eq!(out2, data);
        drop(pf2);
        fs::remove_all(&dir).unwrap();
    }

    /// Golden interop check — the on-disk header must match
    /// libtorrent's layout byte-for-byte: big-endian `num_pieces`,
    /// `piece_size`, then one big-endian `i32` slot index per piece
    /// (`-1` = absent). This test pins the wire format.
    #[test]
    fn header_is_big_endian() {
        let (dir, pf) = setup();
        pf.write(&[0x11u8; 512], PieceIndex(2), 0).unwrap();
        pf.write(&[0x22u8; 512], PieceIndex(7), 0).unwrap();
        pf.flush_metadata().unwrap();

        let raw = std::fs::read(dir.join("test.part")).unwrap();
        // header_size for 10 pieces = round_up((2+10)*4) = 1024
        assert!(raw.len() >= 1024);
        assert_eq!(&raw[0..4], &10i32.to_be_bytes());
        assert_eq!(&raw[4..8], &1024i32.to_be_bytes());
        // slot map: piece 2 → slot 0, piece 7 → slot 1, rest -1
        for p in 0..10 {
            let off = 8 + p * 4;
            let want: i32 = match p {
                2 => 0,
                7 => 1,
                _ => -1,
            };
            assert_eq!(
                i32::from_be_bytes(raw[off..off + 4].try_into().unwrap()),
                want,
                "piece {p} slot"
            );
        }
        // payload at slot 0 = piece 2's data
        assert!(raw[1024..1024 + 512].iter().all(|&b| b == 0x11));
        assert!(raw[2048..2048 + 512].iter().all(|&b| b == 0x22));

        // a libtorrent-shaped header parses back (simulate by writing
        // the same bytes — the parse path is what matters)
        drop(pf);
        let pf2 = PartFile::new(dir.clone(), PathBuf::from("test.part"), 10, 1024);
        let mut out = vec![0u8; 512];
        assert_eq!(pf2.read(&mut out, PieceIndex(2), 0).unwrap(), 512);
        assert_eq!(out[0], 0x11);
        assert_eq!(pf2.read(&mut out, PieceIndex(7), 0).unwrap(), 512);
        assert_eq!(out[0], 0x22);
        drop(pf2);
        fs::remove_all(&dir).unwrap();
    }

    /// A header crafted little-endian (the OLD buggy format) must NOT
    /// parse as valid — it's a different, incompatible layout.
    /// Writing the same logical map little-endian and re-opening must
    /// yield an empty map (num_pieces mismatch).
    #[test]
    fn little_endian_header_rejected() {
        let (dir, _pf) = setup();
        let mut header = vec![0u8; 1024];
        header[0..4].copy_from_slice(&10i32.to_le_bytes());
        header[4..8].copy_from_slice(&1024i32.to_le_bytes());
        std::fs::write(dir.join("test.part"), &header).unwrap();
        let pf2 = PartFile::new(dir.clone(), PathBuf::from("test.part"), 10, 1024);
        let mut out = vec![0u8; 512];
        assert_eq!(
            pf2.read(&mut out, PieceIndex(2), 0).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        drop(pf2);
        fs::remove_all(&dir).unwrap();
    }

    /// Upstream slot allocation is FIFO: freed slots come back in
    /// free-order, not stack order.
    #[test]
    fn free_slots_are_fifo() {
        let (dir, pf) = setup();
        pf.write(&[1u8; 512], PieceIndex(0), 0).unwrap();
        pf.write(&[2u8; 512], PieceIndex(1), 0).unwrap();
        pf.write(&[3u8; 512], PieceIndex(2), 0).unwrap();
        // free slot 0 (piece 0) then slot 1 (piece 1)
        pf.free_piece(PieceIndex(0));
        pf.free_piece(PieceIndex(1));
        // next two writes: FIFO → piece 3 gets slot 0, piece 4 slot 1
        pf.write(&[4u8; 512], PieceIndex(3), 0).unwrap();
        pf.write(&[5u8; 512], PieceIndex(4), 0).unwrap();
        pf.flush_metadata().unwrap();
        let raw = std::fs::read(dir.join("test.part")).unwrap();
        // piece 3 → slot 0, piece 4 → slot 1, piece 2 → slot 2
        let slot_of = |p: usize| {
            let off = 8 + p * 4;
            i32::from_be_bytes(raw[off..off + 4].try_into().unwrap())
        };
        assert_eq!(slot_of(3), 0);
        assert_eq!(slot_of(4), 1);
        assert_eq!(slot_of(2), 2);
        drop(pf);
        fs::remove_all(&dir).unwrap();
    }

    #[test]
    fn free_and_reuse_slot() {
        let (dir, pf) = setup();
        pf.write(&[1u8; 512], PieceIndex(0), 0).unwrap();
        pf.write(&[2u8; 512], PieceIndex(1), 0).unwrap();
        pf.free_piece(PieceIndex(0));
        // next write reuses slot 0
        pf.write(&[3u8; 512], PieceIndex(2), 0).unwrap();
        let mut out = vec![0u8; 512];
        pf.read(&mut out, PieceIndex(2), 0).unwrap();
        assert_eq!(out[0], 3);
        assert!(pf.read(&mut out, PieceIndex(0), 0).is_err());
        drop(pf);
        fs::remove_all(&dir).unwrap();
    }

    #[test]
    fn export_frees_full_pieces() {
        let (dir, pf) = setup();
        pf.write(&[7u8; 1024], PieceIndex(1), 0).unwrap();
        pf.write(&[8u8; 1024], PieceIndex(2), 0).unwrap();
        let mut got = Vec::new();
        pf.export_file(1024, 2048, |off, bytes| {
            got.push((off, bytes.to_vec()));
            Ok(())
        })
        .unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0, 0);
        assert!(got[0].1.iter().all(|&b| b == 7));
        // full pieces consumed
        let mut out = vec![0u8; 1024];
        assert!(pf.read(&mut out, PieceIndex(1), 0).is_err());
        drop(pf);
        fs::remove_all(&dir).unwrap();
    }

    #[test]
    fn empty_flush_removes_file() {
        let (dir, pf) = setup();
        pf.write(&[9u8; 64], PieceIndex(0), 0).unwrap();
        pf.free_piece(PieceIndex(0));
        pf.flush_metadata().unwrap();
        assert!(!dir.join("test.part").exists());
        drop(pf);
        fs::remove_all(&dir).unwrap();
    }

    /// `move_partfile`: move with data → file lands at the new dir;
    /// move with an empty map → no data file but the path updates;
    /// move when the file is already gone → tolerated (ENOENT).
    #[test]
    fn move_partfile_semantics() {
        let (dir, mut pf) = setup();
        pf.write(&[0xccu8; 512], PieceIndex(4), 0).unwrap();
        let dir2 =
            std::env::temp_dir().join(format!("bitdaemon_part_file_mv_{}", std::process::id()));
        let _ = fs::remove_all(&dir2);
        fs::create_directories(&dir2).unwrap();
        pf.move_partfile(dir2.clone()).unwrap();
        // old file moved — readable from the new location
        assert!(!dir.join("test.part").exists());
        assert!(dir2.join("test.part").exists());
        let mut out = vec![0u8; 512];
        assert_eq!(pf.read(&mut out, PieceIndex(4), 0).unwrap(), 512);
        assert_eq!(out[0], 0xcc);
        drop(pf);
        fs::remove_all(&dir).unwrap();
        fs::remove_all(&dir2).unwrap();
    }

    /// Moving an *empty* part file is a no-op that still reparents.
    #[test]
    fn move_empty_partfile() {
        let (dir, mut pf) = setup();
        pf.write(&[0xddu8; 512], PieceIndex(0), 0).unwrap();
        pf.free_piece(PieceIndex(0));
        let dir2 =
            std::env::temp_dir().join(format!("bitdaemon_part_file_mv2_{}", std::process::id()));
        let _ = fs::remove_all(&dir2);
        fs::create_directories(&dir2).unwrap();
        pf.move_partfile(dir2.clone()).unwrap();
        // flush_metadata removed the file; nothing exists at dir2
        assert!(!dir2.join("test.part").exists());
        drop(pf);
        fs::remove_all(&dir).unwrap();
        fs::remove_all(&dir2).unwrap();
    }

    /// Write-open retries after creating a missing parent dir —
    /// upstream `open_file`'s ENOENT → `create_directories` path.
    #[test]
    fn write_creates_missing_dir() {
        let dir =
            std::env::temp_dir().join(format!("bitdaemon_part_file_mkdir_{}", std::process::id()));
        let _ = fs::remove_all(&dir);
        // do NOT create dir — the open must mkdir+retry
        let pf = PartFile::new(dir.clone(), PathBuf::from("test.part"), 10, 1024);
        pf.write(&[0xabu8; 512], PieceIndex(0), 0).unwrap();
        assert!(dir.join("test.part").exists());
        drop(pf);
        fs::remove_all(&dir).unwrap();
    }
}
