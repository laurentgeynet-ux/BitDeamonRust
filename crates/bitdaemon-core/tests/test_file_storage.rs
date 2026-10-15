//! Port of `test_file_storage.cpp` — the `file_storage` cases that don't
//! depend on `canonicalize`/`rename_file` internals (ABI<4-only upstream)
//! plus the symlink and `files_compatible` suites verbatim.

use bitdaemon_common::{FileIndex, PieceIndex};
use bitdaemon_core::file_storage::{
    FileFlags, FileStorage, calc_num_pieces, file_piece_range_exclusive,
    file_piece_range_inclusive, files_compatible,
};
use bitdaemon_core::path::{SEPARATOR, combine_path};

fn add_file(fs: &mut FileStorage, path: &[u8], size: i64) {
    fs.add_file(None, path, size, FileFlags::NONE, 0, b"", None)
        .unwrap();
}

fn add_symlink(fs: &mut FileStorage, path: &[u8], target: &[u8]) {
    fs.add_file(None, path, 0, FileFlags::SYMLINK, 0, target, None)
        .unwrap();
}

fn sep_join(parts: &[&[u8]]) -> Vec<u8> {
    let mut ret = Vec::new();
    for p in parts {
        if !ret.is_empty() {
            ret.push(SEPARATOR);
        }
        ret.extend_from_slice(p);
    }
    ret
}

/// `setup_test_storage` from upstream.
fn setup_test_storage() -> FileStorage {
    let mut st = FileStorage::new();
    add_file(&mut st, &combine_path(b"test", b"a"), 10000);
    add_file(&mut st, &combine_path(b"test", b"b"), 20000);
    add_file(
        &mut st,
        &combine_path(b"test", &combine_path(b"c", b"a")),
        30000,
    );
    add_file(
        &mut st,
        &combine_path(b"test", &combine_path(b"c", b"b")),
        40000,
    );

    st.set_piece_length(0x4000);
    st.set_num_pieces(calc_num_pieces(&st));

    assert_eq!(st.file_name(FileIndex(0)), b"a");
    assert_eq!(st.file_name(FileIndex(1)), b"b");
    assert_eq!(st.file_name(FileIndex(2)), b"a");
    assert_eq!(st.file_name(FileIndex(3)), b"b");
    assert_eq!(st.name(), b"test");

    assert_eq!(st.file_path(FileIndex(0), b""), combine_path(b"test", b"a"));
    assert_eq!(st.file_path(FileIndex(1), b""), combine_path(b"test", b"b"));
    assert_eq!(
        st.file_path(FileIndex(2), b""),
        combine_path(b"test", &combine_path(b"c", b"a"))
    );
    assert_eq!(
        st.file_path(FileIndex(3), b""),
        combine_path(b"test", &combine_path(b"c", b"b"))
    );

    assert_eq!(st.file_size(FileIndex(0)), 10000);
    assert_eq!(st.file_size(FileIndex(1)), 20000);
    assert_eq!(st.file_size(FileIndex(2)), 30000);
    assert_eq!(st.file_size(FileIndex(3)), 40000);

    assert_eq!(st.file_offset(FileIndex(0)), 0);
    assert_eq!(st.file_offset(FileIndex(1)), 10000);
    assert_eq!(st.file_offset(FileIndex(2)), 30000);
    assert_eq!(st.file_offset(FileIndex(3)), 60000);

    assert_eq!(st.total_size(), 100000);
    assert_eq!(st.size_on_disk(), 100000);
    assert_eq!(st.piece_length(), 0x4000);
    assert_eq!(st.num_pieces(), (100000 + 0x3fff) / 0x4000);
    st
}

#[test]
fn test_setup() {
    let _ = setup_test_storage();
}

#[test]
fn test_set_name() {
    let mut st = setup_test_storage();
    st.set_name(b"test_2");
    assert_eq!(
        st.file_path(FileIndex(0), b"."),
        combine_path(b".", &combine_path(b"test_2", b"a"))
    );
}

#[test]
fn test_invalid_path1() {
    let mut st = FileStorage::new();
    st.set_piece_length(16 * 1024);
    if cfg!(windows) {
        add_file(&mut st, br"+\\\(", 10);
    } else {
        add_file(&mut st, b"+///(", 10);
    }

    assert_eq!(st.file_name(FileIndex(0)), b"(");
    assert_eq!(st.file_path(FileIndex(0), b""), combine_path(b"+", b"("));
}

#[test]
fn test_invalid_path2() {
    let mut st = FileStorage::new();
    st.set_piece_length(16 * 1024);
    if cfg!(windows) {
        add_file(&mut st, br"+\\+\\(", 10);
    } else {
        add_file(&mut st, b"+///+//(", 10);
    }

    assert_eq!(st.file_name(FileIndex(0)), b"(");
    assert_eq!(
        st.file_path(FileIndex(0), b""),
        combine_path(b"+", &combine_path(b"+", b"("))
    );
}

#[test]
fn test_map_file() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(512);
    add_file(&mut fs, &combine_path(b"temp_storage", b"test1.tmp"), 17);
    add_file(&mut fs, &combine_path(b"temp_storage", b"test2.tmp"), 612);
    add_file(&mut fs, &combine_path(b"temp_storage", b"test3.tmp"), 0);
    add_file(&mut fs, &combine_path(b"temp_storage", b"test4.tmp"), 0);
    add_file(&mut fs, &combine_path(b"temp_storage", b"test5.tmp"), 3253);
    // size: 3882
    add_file(&mut fs, &combine_path(b"temp_storage", b"test6.tmp"), 841);
    // size: 4723

    let rq = fs.map_file(FileIndex(0), 0, 10);
    assert_eq!(rq.piece, PieceIndex(0));
    assert_eq!(rq.start, 0);
    assert_eq!(rq.length, 10);
    let rq = fs.map_file(FileIndex(5), 0, 10);
    assert_eq!(rq.piece, PieceIndex(7));
    assert_eq!(rq.start, 298);
    assert_eq!(rq.length, 10);
    let rq = fs.map_file(FileIndex(5), 0, 1000);
    assert_eq!(rq.piece, PieceIndex(7));
    assert_eq!(rq.start, 298);
    assert_eq!(rq.length, 841);
}

#[test]
fn test_piece_range_exclusive() {
    let piece_size: i64 = 16;
    let mut fs = FileStorage::new();
    fs.set_piece_length(piece_size as i32);
    add_file(&mut fs, &combine_path(b"temp_storage", b"0"), piece_size);
    add_file(
        &mut fs,
        &combine_path(b"temp_storage", b"1"),
        piece_size * 4 + 1,
    );
    add_file(
        &mut fs,
        &combine_path(b"temp_storage", b"2"),
        piece_size * 4 - 1,
    );
    fs.set_num_pieces(calc_num_pieces(&fs));
    //        +---+---+---+---+---+---+---+---+---+
    // pieces | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 |
    //        +---+---+---+---+---+---+---+---+---+
    // files  | 0 |        1       |        2     |
    //        +---+----------------+--------------+

    let r = file_piece_range_exclusive(&fs, FileIndex(0));
    assert_eq!((r.begin, r.end), (PieceIndex(0), PieceIndex(1)));
    let r = file_piece_range_exclusive(&fs, FileIndex(1));
    assert_eq!((r.begin, r.end), (PieceIndex(1), PieceIndex(5)));
    let r = file_piece_range_exclusive(&fs, FileIndex(2));
    assert_eq!((r.begin, r.end), (PieceIndex(6), PieceIndex(9)));
}

#[test]
fn test_piece_range_inclusive() {
    let piece_size: i64 = 16;
    let mut fs = FileStorage::new();
    fs.set_piece_length(piece_size as i32);
    add_file(&mut fs, &combine_path(b"temp_storage", b"0"), piece_size);
    add_file(
        &mut fs,
        &combine_path(b"temp_storage", b"1"),
        piece_size * 4 + 1,
    );
    add_file(
        &mut fs,
        &combine_path(b"temp_storage", b"2"),
        piece_size * 4 - 1,
    );
    fs.set_num_pieces(calc_num_pieces(&fs));

    let r = file_piece_range_inclusive(&fs, FileIndex(0));
    assert_eq!((r.begin, r.end), (PieceIndex(0), PieceIndex(1)));
    let r = file_piece_range_inclusive(&fs, FileIndex(1));
    assert_eq!((r.begin, r.end), (PieceIndex(1), PieceIndex(6)));
    let r = file_piece_range_inclusive(&fs, FileIndex(2));
    assert_eq!((r.begin, r.end), (PieceIndex(5), PieceIndex(9)));
}

#[test]
fn test_piece_range() {
    let piece_size = 0x4000i64;
    let mut fs = FileStorage::new();
    fs.set_piece_length(piece_size as i32);
    add_file(
        &mut fs,
        &combine_path(b"temp_storage", b"0"),
        piece_size * 3,
    );
    add_file(
        &mut fs,
        &combine_path(b"temp_storage", b"1"),
        piece_size * 3 + 0x30,
    );
    fs.set_num_pieces(calc_num_pieces(&fs));

    let r = file_piece_range_inclusive(&fs, FileIndex(0));
    assert_eq!((r.begin, r.end), (PieceIndex(0), PieceIndex(3)));
    let r = file_piece_range_inclusive(&fs, FileIndex(1));
    assert_eq!((r.begin, r.end), (PieceIndex(3), PieceIndex(7)));

    let r = file_piece_range_exclusive(&fs, FileIndex(0));
    assert_eq!((r.begin, r.end), (PieceIndex(0), PieceIndex(3)));
    let r = file_piece_range_exclusive(&fs, FileIndex(1));
    assert_eq!((r.begin, r.end), (PieceIndex(3), PieceIndex(7)));
}

/// upstream `piece_range_short_file_owns_no_piece` (e7381c7bc): a file
/// starting mid-piece that never reaches that piece's end shares it with
/// both neighbors — its *exclusive* range must be empty `{2,2}`, not the
/// inverted `{2,1}` the unclamped formula produced.
#[test]
fn test_piece_range_short_file_owns_no_piece() {
    let piece_size: i64 = 16;
    let mut fs = FileStorage::new();
    fs.set_piece_length(piece_size as i32);
    // "middle" starts mid-piece (piece 1, local offset 4) and ends at
    // local offset 7 — it shares piece 1 with both neighbors and owns
    // no piece exclusively
    add_file(&mut fs, &combine_path(b"temp_storage", b"0"), 20);
    add_file(&mut fs, &combine_path(b"temp_storage", b"middle"), 3);
    add_file(&mut fs, &combine_path(b"temp_storage", b"2"), 17);
    fs.set_num_pieces(calc_num_pieces(&fs));
    //        +----------------+--+-------------------------+
    // pieces | 0              | 1                | 2       |
    //        +----------------+--+-------------------------+
    // files  |      0         |m |            2            |
    //        +----------------+--+-------------------------+
    assert_eq!(fs.num_pieces(), 3);

    // "0" fully owns piece 0; piece 1 is split between "0", "middle", "2"
    let r = file_piece_range_inclusive(&fs, FileIndex(0));
    assert_eq!((r.begin, r.end), (PieceIndex(0), PieceIndex(2)));
    let r = file_piece_range_exclusive(&fs, FileIndex(0));
    assert_eq!((r.begin, r.end), (PieceIndex(0), PieceIndex(1)));

    // "middle" only touches piece 1 (inclusive well-formed); exclusive
    // must be the empty `{2,2}` — not inverted
    let r = file_piece_range_inclusive(&fs, FileIndex(1));
    assert_eq!((r.begin, r.end), (PieceIndex(1), PieceIndex(2)));
    let r = file_piece_range_exclusive(&fs, FileIndex(1));
    assert_eq!((r.begin, r.end), (PieceIndex(2), PieceIndex(2)));

    // "2" is the last file: owns piece 2 exclusively (piece 1 shared)
    let r = file_piece_range_inclusive(&fs, FileIndex(2));
    assert_eq!((r.begin, r.end), (PieceIndex(1), PieceIndex(3)));
    let r = file_piece_range_exclusive(&fs, FileIndex(2));
    assert_eq!((r.begin, r.end), (PieceIndex(2), PieceIndex(3)));
}

#[test]
fn test_piece_size_last_piece() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(1024);
    add_file(&mut fs, b"0", 100);
    fs.set_num_pieces(calc_num_pieces(&fs));
    assert_eq!(fs.piece_size(PieceIndex(0)), 100);
}

#[test]
fn test_piece_size_middle_piece() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(1024);
    add_file(&mut fs, b"0", 2000);
    fs.set_num_pieces(calc_num_pieces(&fs));
    assert_eq!(fs.piece_size(PieceIndex(0)), 1024);
    assert_eq!(fs.piece_size(PieceIndex(1)), 2000 - 1024);
}

#[test]
fn test_file_index_at_offset() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(1024);
    add_file(&mut fs, b"test/0", 1);
    add_file(&mut fs, b"test/1", 2);
    add_file(&mut fs, b"test/2", 3);
    add_file(&mut fs, b"test/3", 4);
    add_file(&mut fs, b"test/4", 5);
    for (offset, f) in [0, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4]
        .into_iter()
        .enumerate()
    {
        assert_eq!(fs.file_index_at_offset(offset as i64), FileIndex(f));
    }
}

#[test]
fn test_map_block_start() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(1024);
    add_file(&mut fs, b"test/0", 1);
    add_file(&mut fs, b"test/1", 2);
    add_file(&mut fs, b"test/2", 3);
    add_file(&mut fs, b"test/3", 4);
    add_file(&mut fs, b"test/4", 5);
    fs.set_num_pieces(calc_num_pieces(&fs));
    for (len, f) in [0, 1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5]
        .into_iter()
        .enumerate()
    {
        let map = fs.map_block(PieceIndex(0), 0, len as i64);
        assert_eq!(map.len() as i32, f);
        let mut actual_len = 0i64;
        for (file_index, file) in map.iter().enumerate() {
            assert_eq!(file.file_index, FileIndex(file_index as i32));
            assert_eq!(file.offset, 0);
            actual_len += file.size;
        }
        assert_eq!(actual_len, len as i64);
    }
}

#[test]
fn test_map_block_mid() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(1024);
    add_file(&mut fs, b"test/0", 1);
    add_file(&mut fs, b"test/1", 2);
    add_file(&mut fs, b"test/2", 3);
    add_file(&mut fs, b"test/3", 4);
    add_file(&mut fs, b"test/4", 5);
    fs.set_num_pieces(calc_num_pieces(&fs));
    for (offset, f) in [0, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4]
        .into_iter()
        .enumerate()
    {
        let offset = offset as i64;
        let map = fs.map_block(PieceIndex(0), offset, 1);
        assert_eq!(map.len(), 1);
        let file = map[0];
        assert_eq!(file.file_index, FileIndex(f));
        assert!(file.offset <= offset);
        assert_eq!(file.size, 1);
    }
}

#[test]
fn test_sanitize_symlinks() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(1024);

    // invalid
    if cfg!(windows) {
        add_symlink(&mut fs, b"test/0", br"C:\invalid\target\path");
    } else {
        add_symlink(&mut fs, b"test/0", b"/invalid/target/path");
    }

    // there is no file with this name, so this is invalid
    add_symlink(&mut fs, b"test/1", b"ZZ");

    // there is no file with this name, so this is invalid
    add_symlink(&mut fs, b"test/2", &sep_join(&[b"B", b"B", b"ZZ"]));

    // this should be OK
    add_symlink(&mut fs, b"test/3", b"0");

    // this should be OK
    add_symlink(&mut fs, b"test/4", b"A");

    // this is advanced, but OK
    add_symlink(&mut fs, b"test/5", &sep_join(&[b"4", b"B"]));

    // this is advanced, but OK
    add_symlink(&mut fs, b"test/6", &sep_join(&[b"5", b"C"]));

    // this is not OK
    add_symlink(&mut fs, b"test/7", &sep_join(&[b"4", b"B", b"C", b"ZZ"]));

    // this is the only actual content
    add_file(&mut fs, &sep_join(&[b"test", b"A", b"B", b"C"]), 10000);
    fs.set_num_pieces(((fs.total_size() + 1023) / 1024) as i32);

    fs.sanitize_symlinks();

    // these were all invalid symlinks, so they're made to point to
    // themselves
    assert_eq!(fs.symlink(FileIndex(0)), sep_join(&[b"test", b"0"]));
    assert_eq!(fs.symlink(FileIndex(1)), sep_join(&[b"test", b"1"]));
    assert_eq!(fs.symlink(FileIndex(2)), sep_join(&[b"test", b"2"]));

    // ok
    assert_eq!(fs.symlink(FileIndex(3)), sep_join(&[b"test", b"0"]));
    assert_eq!(fs.symlink(FileIndex(4)), sep_join(&[b"test", b"A"]));
    assert_eq!(fs.symlink(FileIndex(5)), sep_join(&[b"test", b"4", b"B"]));
    assert_eq!(fs.symlink(FileIndex(6)), sep_join(&[b"test", b"5", b"C"]));

    // does not point to a valid file
    assert_eq!(fs.symlink(FileIndex(7)), sep_join(&[b"test", b"7"]));
}

#[test]
fn test_sanitize_symlinks_single_file() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(1024);
    add_file(&mut fs, b"test", 1);
    fs.set_num_pieces(((fs.total_size() + 1023) / 1024) as i32);

    fs.sanitize_symlinks();

    assert_eq!(fs.file_path(FileIndex(0), b""), b"test");
}

#[test]
fn test_sanitize_symlinks_cascade() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(1024);

    add_symlink(&mut fs, b"test/0", &sep_join(&[b"1", b"ZZ"]));
    add_symlink(&mut fs, b"test/1", b"2");
    add_symlink(&mut fs, b"test/2", b"3");
    add_symlink(&mut fs, b"test/3", b"4");
    add_symlink(&mut fs, b"test/4", b"5");
    add_symlink(&mut fs, b"test/5", b"6");
    add_symlink(&mut fs, b"test/6", b"7");
    add_symlink(&mut fs, b"test/7", b"A");
    add_symlink(&mut fs, b"test/no-exist", &sep_join(&[b"1", b"ZZZ"]));

    // this is the only actual content
    add_file(&mut fs, &sep_join(&[b"test", b"A", b"ZZ"]), 10000);
    fs.set_num_pieces(((fs.total_size() + 1023) / 1024) as i32);

    fs.sanitize_symlinks();

    assert_eq!(fs.symlink(FileIndex(0)), sep_join(&[b"test", b"1", b"ZZ"]));
    assert_eq!(fs.symlink(FileIndex(1)), sep_join(&[b"test", b"2"]));
    assert_eq!(fs.symlink(FileIndex(2)), sep_join(&[b"test", b"3"]));
    assert_eq!(fs.symlink(FileIndex(3)), sep_join(&[b"test", b"4"]));
    assert_eq!(fs.symlink(FileIndex(4)), sep_join(&[b"test", b"5"]));
    assert_eq!(fs.symlink(FileIndex(5)), sep_join(&[b"test", b"6"]));
    assert_eq!(fs.symlink(FileIndex(6)), sep_join(&[b"test", b"7"]));
    assert_eq!(fs.symlink(FileIndex(7)), sep_join(&[b"test", b"A"]));
    assert_eq!(fs.symlink(FileIndex(8)), sep_join(&[b"test", b"no-exist"]));
}

#[test]
fn test_sanitize_symlinks_circular() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(1024);

    add_symlink(&mut fs, b"test/0", b"1");
    add_symlink(&mut fs, b"test/1", b"0");

    // when this is resolved, we end up in an infinite loop. Make sure we
    // can handle that
    add_symlink(&mut fs, b"test/2", &sep_join(&[b"0", b"ZZ"]));

    // this is the only actual content
    add_file(&mut fs, &sep_join(&[b"test", b"A", b"ZZ"]), 10000);
    fs.set_num_pieces(((fs.total_size() + 1023) / 1024) as i32);

    fs.sanitize_symlinks();

    assert_eq!(fs.symlink(FileIndex(0)), sep_join(&[b"test", b"1"]));
    assert_eq!(fs.symlink(FileIndex(1)), sep_join(&[b"test", b"0"]));

    // this was invalid, so it points to itself
    assert_eq!(fs.symlink(FileIndex(2)), sep_join(&[b"test", b"2"]));
}

#[test]
fn test_sanitize_symlinks_lexicographic() {
    // an `is_directory` based on a single lower_bound() would miss
    // directories whose name is a strict prefix of another paths entry,
    // when a sibling path sorts between them. e.g. paths {"A.bar", "A/B"}:
    // "A.bar" sorts before "A/B" because '.' (0x2E) < '/' (0x2F), so a
    // search for "A" lands on "A.bar" and would incorrectly conclude "A"
    // is not a directory symlink.
    let mut fs = FileStorage::new();
    fs.set_piece_length(1024);

    add_file(&mut fs, &sep_join(&[b"test", b"A.bar", b"leaf"]), 100);
    add_file(&mut fs, &sep_join(&[b"test", b"A", b"B", b"leaf"]), 100);
    add_symlink(&mut fs, &sep_join(&[b"test", b"sym"]), b"A");

    fs.set_num_pieces(((fs.total_size() + 1023) / 1024) as i32);
    fs.sanitize_symlinks();

    // the symlink resolves to the "A" directory in the torrent
    assert_eq!(fs.symlink(FileIndex(2)), sep_join(&[b"test", b"A"]));
}

#[test]
fn test_query_symlinks() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(1024);
    add_symlink(&mut fs, b"test/0", b"0");
    add_symlink(&mut fs, b"test/1", b"1");
    add_symlink(&mut fs, b"test/2", b"2");
    add_symlink(&mut fs, b"test/3", b"3");

    let ret1 = fs.symlink(FileIndex(0));
    let ret2 = fs.symlink(FileIndex(1));
    let ret3 = fs.symlink(FileIndex(2));
    let ret4 = fs.symlink(FileIndex(3));

    assert_ne!(ret1, ret2);
    assert_ne!(ret1, ret3);
    assert_ne!(ret1, ret4);
    assert_ne!(ret2, ret3);
    assert_ne!(ret2, ret4);
    assert_ne!(ret3, ret4);
}

#[test]
fn test_query_symlinks2() {
    let mut fs = FileStorage::new();
    fs.set_piece_length(1024);
    add_file(&mut fs, b"test/0", 10);
    add_file(&mut fs, b"test/1", 10);
    add_file(&mut fs, b"test/2", 10);
    add_file(&mut fs, b"test/3", 10);

    assert!(fs.symlink(FileIndex(0)).is_empty());
    assert!(fs.symlink(FileIndex(1)).is_empty());
    assert!(fs.symlink(FileIndex(2)).is_empty());
    assert!(fs.symlink(FileIndex(3)).is_empty());
}

#[test]
fn test_file_path_hash() {
    // "Foo" and "foo" must collide — hashing is over lowercased paths
    let mut fs = FileStorage::new();
    fs.set_piece_length(512);
    add_file(&mut fs, &combine_path(b"temp_storage", b"Foo"), 17);
    add_file(&mut fs, &combine_path(b"temp_storage", b"foo"), 612);

    let h0 = fs.file_path_hash(FileIndex(0), b"a");
    let h1 = fs.file_path_hash(FileIndex(1), b"a");
    assert_eq!(h0, h1);
}

#[test]
fn test_files_compatible() {
    let mut fs1 = FileStorage::new();
    fs1.set_piece_length(0x4000);
    add_file(&mut fs1, b"test/0", 1);
    add_file(&mut fs1, b"test/1", 2);

    let mut fs2 = FileStorage::new();
    fs2.set_piece_length(0x4000);
    add_file(&mut fs2, b"test/0", 1);
    add_file(&mut fs2, b"test/1", 2);

    assert!(files_compatible(&fs1, &fs2));
}

#[test]
fn test_files_compatible_num_files() {
    let mut fs1 = FileStorage::new();
    fs1.set_piece_length(0x4000);
    add_file(&mut fs1, b"test/0", 1);
    add_file(&mut fs1, b"test/1", 2);

    let mut fs2 = FileStorage::new();
    fs2.set_piece_length(0x4000);
    add_file(&mut fs2, b"test/0", 3);

    assert!(!files_compatible(&fs1, &fs2));
}

#[test]
fn test_files_compatible_size() {
    let mut fs1 = FileStorage::new();
    fs1.set_piece_length(0x4000);
    add_file(&mut fs1, b"test/0", 2);
    add_file(&mut fs1, b"test/1", 1);

    let mut fs2 = FileStorage::new();
    fs2.set_piece_length(0x4000);
    add_file(&mut fs2, b"test/0", 1);
    add_file(&mut fs2, b"test/1", 2);

    assert!(!files_compatible(&fs1, &fs2));
}

#[test]
fn test_files_compatible_name() {
    let mut fs1 = FileStorage::new();
    fs1.set_piece_length(0x4000);
    add_file(&mut fs1, b"test/1", 1);
    add_file(&mut fs1, b"test/0", 2);

    let mut fs2 = FileStorage::new();
    fs2.set_piece_length(0x4000);
    add_file(&mut fs2, b"test/0", 1);
    add_file(&mut fs2, b"test/1", 2);

    assert!(!files_compatible(&fs1, &fs2));
}

#[test]
fn test_files_compatible_hidden() {
    let mut fs1 = FileStorage::new();
    fs1.set_piece_length(0x4000);
    add_file(&mut fs1, b"test/0", 1);
    fs1.add_file(None, b"test/1", 2, FileFlags::HIDDEN, 0, b"", None)
        .unwrap();

    let mut fs2 = FileStorage::new();
    fs2.set_piece_length(0x4000);
    add_file(&mut fs2, b"test/0", 1);
    add_file(&mut fs2, b"test/1", 2);

    // hidden attribute does not affect compatibility
    assert!(files_compatible(&fs1, &fs2));
}

#[test]
fn test_files_compatible_pad() {
    let mut fs1 = FileStorage::new();
    fs1.set_piece_length(0x4000);
    add_file(&mut fs1, b"test/0", 1);
    fs1.add_file(None, b"test/1", 2, FileFlags::PAD_FILE, 0, b"", None)
        .unwrap();

    let mut fs2 = FileStorage::new();
    fs2.set_piece_length(0x4000);
    add_file(&mut fs2, b"test/0", 1);
    add_file(&mut fs2, b"test/1", 2);

    // pad attribute does affect compatibility
    assert!(!files_compatible(&fs1, &fs2));
}

#[test]
fn test_files_compatible_empty_file_order() {
    let mut fs1 = FileStorage::new();
    fs1.set_piece_length(0x4000);
    add_file(&mut fs1, b"test/0", 1);
    add_file(&mut fs1, b"test/1", 0);
    add_file(&mut fs1, b"test/2", 0);
    add_file(&mut fs1, b"test/3", 0);

    let mut fs2 = FileStorage::new();
    fs2.set_piece_length(0x4000);
    add_file(&mut fs2, b"test/0", 1);
    add_file(&mut fs2, b"test/3", 0);
    add_file(&mut fs2, b"test/2", 0);
    add_file(&mut fs2, b"test/1", 0);

    // order of empty files does not affect compatibility
    assert!(files_compatible(&fs1, &fs2));
}

#[test]
fn add_file_size_and_name_errors() {
    // upstream `add_file_borrow_impl` (`file_storage.cpp:819-843`):
    // per-file size cap → `errc::file_too_large`; cumulative offset
    // overflow → `torrent_invalid_length`; leaf ≥ 4096 → `filename_too_long`
    use bitdaemon_core::{Error, TorrentError};

    let mut st = FileStorage::new();
    st.set_piece_length(0x4000);

    let err = st
        .add_file(
            None,
            b"f",
            FileStorage::MAX_FILE_SIZE + 1,
            FileFlags::NONE,
            0,
            b"",
            None,
        )
        .unwrap_err();
    assert!(matches!(err, Error::FileTooLarge), "{err:?}");

    add_file(&mut st, b"dir/f", 100);
    let err = st
        .add_file(
            None,
            b"dir/g",
            FileStorage::MAX_FILE_OFFSET - 50,
            FileFlags::NONE,
            0,
            b"",
            None,
        )
        .unwrap_err();
    assert!(
        matches!(err, Error::Torrent(TorrentError::TorrentInvalidLength)),
        "{err:?}"
    );

    let mut st2 = FileStorage::new();
    st2.set_piece_length(0x4000);
    let long_name = vec![b'a'; 4096];
    let err = st2
        .add_file(None, &long_name, 10, FileFlags::NONE, 0, b"", None)
        .unwrap_err();
    assert!(matches!(err, Error::FilenameTooLong), "{err:?}");
}
