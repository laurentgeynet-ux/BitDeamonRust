//! Conformance port of `TORRENT_TEST(resolve_duplicates)` in
//! `test_torrent_info.cpp` — case-insensitive collision resolution via
//! the full `create_torrent` → `load_torrent` → `renamed_files` path.

use bitdaemon_common::Sha1Hash;
use bitdaemon_common::types::PieceIndex;
use bitdaemon_core::create_torrent::{CreateFileEntry, CreateFlags, CreateTorrent};
use bitdaemon_core::file_storage::FileFlags;
use bitdaemon_core::torrent_info::{LoadTorrentLimits, load_torrent_buffer};

struct FileT {
    filename: &'static str,
    size: i64,
    flags: FileFlags,
    expected: &'static str,
}

fn run_case(case: &[FileT]) {
    let files: Vec<CreateFileEntry> = case
        .iter()
        .map(|f| CreateFileEntry {
            filename: f.filename.as_bytes().to_vec(),
            size: f.size,
            flags: f.flags,
            mtime: 0,
            symlink: Vec::new(),
        })
        .collect();
    // duplicates aren't supported by v2 — test via v1_only like upstream
    let mut t = CreateTorrent::new(files, 0x4000, CreateFlags::V1_ONLY).unwrap();
    for i in 0..t.num_pieces() {
        t.set_hash(PieceIndex(i), Sha1Hash::from_bytes(&[0xffu8; 20]).unwrap())
            .unwrap();
    }
    let buf = t.generate_buf().unwrap();
    let atp = load_torrent_buffer(&buf, &LoadTorrentLimits::default()).unwrap();
    let ti = atp.ti.as_ref().unwrap();
    for (i, f) in case.iter().enumerate() {
        let idx = bitdaemon_common::FileIndex(i as i32);
        let got: Vec<u8> = atp
            .renamed_files
            .get(&(i as i32))
            .cloned()
            .unwrap_or_else(|| ti.files().file_path(idx, b""));
        // convert_path_to_posix — the resolver works on platform
        // separators, upstream compares after conversion
        let got_posix: String = String::from_utf8_lossy(&got).replace('\\', "/");
        assert_eq!(got_posix, f.expected, "file {} ({})", i, f.filename);
    }
}

macro_rules! cases {
    ($($name:ident => [$($f:tt)*])*) => {$(
        #[test]
        fn $name() { run_case(&[$($f)*]); }
    )*};
}

fn f(filename: &'static str, expected: &'static str) -> FileT {
    FileT {
        filename,
        size: 0x4000,
        flags: FileFlags::NONE,
        expected,
    }
}
fn pad(filename: &'static str, size: i64, expected: &'static str) -> FileT {
    FileT {
        filename,
        size,
        flags: FileFlags::PAD_FILE,
        expected,
    }
}

cases! {
    duplicates_basic => [
        f("test/temporary.txt", "test/temporary.txt"),
        f("test/Temporary.txt", "test/Temporary.1.txt"),
        f("test/TeMPorArY.txT", "test/TeMPorArY.2.txT"),
        f("test/test/TEMPORARY.TXT", "test/test/TEMPORARY.TXT"),
    ]
    duplicates_exe => [
        f("test/b.exe", "test/b.exe"),
        f("test/B.ExE", "test/B.1.ExE"),
        f("test/B.exe", "test/B.2.exe"),
        f("test/filler", "test/filler"),
    ]
    duplicates_nested => [
        f("test/a/b/c/d/e/f/g/h/i/j/k/l/m", "test/a/b/c/d/e/f/g/h/i/j/k/l/m"),
        f("test/a", "test/a.1"),
        f("test/a/b", "test/a/b.1"),
        f("test/a/b/c", "test/a/b/c.1"),
        f("test/a/b/c/d", "test/a/b/c/d.1"),
        f("test/a/b/c/d/e", "test/a/b/c/d/e.1"),
        f("test/a/b/c/d/e/f", "test/a/b/c/d/e/f.1"),
        f("test/a/b/c/d/e/f/g", "test/a/b/c/d/e/f/g.1"),
        f("test/a/b/c/d/e/f/g/h", "test/a/b/c/d/e/f/g/h.1"),
        f("test/a/b/c/d/e/f/g/h/i", "test/a/b/c/d/e/f/g/h/i.1"),
        f("test/a/b/c/d/e/f/g/h/i/j", "test/a/b/c/d/e/f/g/h/i/j.1"),
    ]
    dir_takes_precedence => [
        f("test/a", "test/a.1"),
        f("test/a/b", "test/a/b"),
    ]
    file_vs_dir => [
        f("test/A/tmp", "test/A/tmp"),
        f("test/a", "test/a.1"),
        f("test/A", "test/A.2"),
        f("test/filler", "test/filler"),
    ]
    subset_collision => [
        f("test/long/path/name/that/collides", "test/long/path/name/that/collides"),
        f("test/long/path", "test/long/path.1"),
        f("test/filler-1", "test/filler-1"),
        f("test/filler-2", "test/filler-2"),
    ]
    pad_same_size_ok => [
        pad("test/.pad/1234", 0x4000, "test/.pad/1234"),
        f("test/filler-1", "test/filler-1"),
        pad("test/.pad/1234", 0x4000, "test/.pad/1234"),
        f("test/filler-2", "test/filler-2"),
    ]
    pad_diff_size_renamed => [
        pad("test/.pad/1234", 0x8000, "test/.pad/1234"),
        f("test/filler-1", "test/filler-1"),
        pad("test/.pad/1234", 0x4000, "test/.pad/1234.1"),
        f("test/filler-2", "test/filler-2"),
    ]
    pad_vs_normal => [
        f("test/.pad/1234", "test/.pad/1234"),
        f("test/filler-1", "test/filler-1"),
        pad("test/.pad/1234", 0x4000, "test/.pad/1234.1"),
        f("test/filler-2", "test/filler-2"),
    ]
    normal_vs_pad => [
        pad("test/.pad/1234", 0x4000, "test/.pad/1234"),
        f("test/filler-1", "test/filler-1"),
        f("test/.pad/1234", "test/.pad/1234.1"),
        f("test/filler-2", "test/filler-2"),
    ]
    pad_vs_dir => [
        pad("test/.pad/1234", 0x4000, "test/.pad/1234.1"),
        f("test/filler-1", "test/filler-1"),
        f("test/.pad/1234/filler-2", "test/.pad/1234/filler-2"),
    ]
    renamed_collision => [
        f("test/temporary.txt", "test/temporary.txt"),
        f("test/Temporary.txt", "test/Temporary.1.txt"),
        // literal name collides with the renamed one → must be renamed too
        f("test/TEMPORARY.1.TXT", "test/TEMPORARY.1.1.TXT"),
    ]
}
