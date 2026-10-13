//! Port of `aux_/part_file.hpp`/`part_file.cpp`.
//!
//! On-disk layout (unchanged from upstream):
//! `[u32 num_pieces][u32 piece_size][i32 slot_of_piece × num_pieces][pad]`
//! header rounded up to a whole KiB; payload slots start at
//! `header_size` and each hold one `piece_size` block.

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
        assert!(num_pieces > 0);
        assert!(piece_size > 0);
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

        // parse header of existing file
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

    fn parse_header(&self, header: &[u8]) {
        let num_pieces = i32::from_le_bytes(header[0..4].try_into().unwrap_or_default());
        let piece_size = i32::from_le_bytes(header[4..8].try_into().unwrap_or_default());
        // mismatch → treat as empty
        if num_pieces != self.max_pieces || piece_size != self.piece_size {
            return;
        }
        let mut g = self.lock();
        let mut free_slots = vec![true; usize::try_from(self.max_pieces).unwrap_or(0)];
        let mut ptr = 8;
        for piece in 0..self.max_pieces {
            let slot = i32::from_le_bytes(header[ptr..ptr + 4].try_into().unwrap_or_default());
            ptr += 4;
            if slot < 0 {
                continue;
            }
            // invalid part-file
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

    /// `allocate_slot` — pick a free slot or extend the file.
    fn allocate_slot(g: &mut Locked, piece: PieceIndex) -> SlotIndex {
        debug_assert!(!g.piece_map.contains_key(&piece.0));
        let slot = g.free_slots.pop().unwrap_or_else(|| {
            let s = g.num_allocated;
            g.num_allocated += 1;
            s
        });
        g.dirty_metadata = true;
        g.piece_map.insert(piece.0, slot);
        slot
    }

    fn slot_offset(&self, slot: SlotIndex) -> i64 {
        i64::from(slot) * i64::from(self.piece_size) + self.header_size
    }

    fn open_file(&self, mode: OpenMode) -> io::Result<FileHandle> {
        let fnm = self.path.join(&self.name);
        FileHandle::open(&fnm, 0, mode).map_err(|e| e.source)
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
    pub fn writev(&self, bufs: &[&[u8]], piece: PieceIndex, offset: i32) -> io::Result<i32> {
        let slot = {
            let mut g = self.lock();
            match g.piece_map.get(&piece.0) {
                Some(&s) => s,
                None => Self::allocate_slot(&mut g, piece),
            }
        };
        let f = self.open_file(OpenMode::WRITE)?;
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
        let slot = self
            .lock()
            .piece_map
            .get(&piece.0)
            .copied()
            .ok_or_else(missing_piece)?;
        let f = self.open_file(OpenMode::READ_ONLY)?;
        let file_offset = self.slot_offset(slot) + i64::from(offset);
        let n = pread_all(f.file(), buf, u64::try_from(file_offset).unwrap_or(0))?;
        Ok(i32::try_from(n).unwrap_or(0))
    }

    /// `hash(ph, len, piece, offset)` — SHA-1 of `len` bytes read from
    /// `piece` at `offset` (fed through `ph.update` by caller? no —
    /// upstream updates the hasher directly). We return the bytes so
    /// the caller can feed any hasher — same observable contract:
    /// `partfile_missing_piece` on absent piece.
    pub fn hash_feed(&self, piece: PieceIndex, offset: i32, len: usize) -> io::Result<Vec<u8>> {
        let mut buf = vec![0u8; len];
        self.read(&mut buf, piece, offset)?;
        Ok(buf)
    }

    /// `free_piece` — drop the slot backing `piece`; returns it to the
    /// free list.
    pub fn free_piece(&self, piece: PieceIndex) {
        let mut g = self.lock();
        if let Some(slot) = g.piece_map.remove(&piece.0) {
            g.free_slots.push(slot);
            g.dirty_metadata = true;
        }
    }

    /// `move_partfile(path)` — flush + rename; new dir must exist.
    pub fn move_partfile(&mut self, path: PathBuf) -> io::Result<()> {
        self.flush_metadata()?;
        let old = self.path.join(&self.name);
        let new = path.join(&self.name);
        if old != new {
            // upstream renames; rename fails if dst exists
            fs::rename(&old, &new)?;
        }
        self.path = path;
        Ok(())
    }

    /// `export_file` — call `f(offset, bytes)` for each stored block
    /// covering `[offset, offset+size)`; pieces fully inside the range
    /// are *freed* afterwards (upstream erases full-piece slots).
    pub fn export_file(
        &self,
        offset: i64,
        size: i64,
        mut f: impl FnMut(i64, &[u8]),
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
        let mut buf: Option<Vec<u8>> = None;
        let file = self.open_file(OpenMode::READ_ONLY)?;

        while piece < end_piece {
            let slot = {
                let g = self.lock();
                g.piece_map.get(&piece.0).copied()
            };
            let block_to_copy = i64::min(piece_size - piece_offset, size - file_offset);
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
                );

                if block_to_copy == piece_size {
                    let mut g = self.lock();
                    if g.piece_map.get(&piece.0) == Some(&slot) {
                        g.free_slots.push(slot);
                        g.piece_map.remove(&piece.0);
                        g.dirty_metadata = true;
                    }
                }
            }
            file_offset += block_to_copy;
            piece_offset = 0;
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
        header[0..4].copy_from_slice(&max_pieces.to_le_bytes());
        header[4..8].copy_from_slice(&self.piece_size.to_le_bytes());
        {
            let g = self.lock();
            let mut ptr = 8;
            for piece in 0..max_pieces {
                let slot = g.piece_map.get(&piece).copied().unwrap_or(-1);
                header[ptr..ptr + 4].copy_from_slice(&slot.to_le_bytes());
                ptr += 4;
            }
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
}
