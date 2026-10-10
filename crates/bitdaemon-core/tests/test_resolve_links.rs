//! Conformance test for `ResolveLinks` — upstream `test_resolve_links.cpp`
//! (fixture table from `test/mutable_test_torrents`).

use std::path::PathBuf;
use std::sync::Arc;

use bitdaemon_core::file_storage::RenamedFiles;
use bitdaemon_core::resolve_links::ResolveLinks;
use bitdaemon_core::torrent_info::load_torrent;

fn fixture_dir() -> Option<PathBuf> {
    let dir =
        PathBuf::from(std::env::var("LIBTORRENT_MUTABLE_DIR").unwrap_or_else(|_| {
            r"D:\Projet\libtorrent\libtorrent\test\mutable_test_torrents".into()
        }));
    dir.is_dir().then_some(dir)
}

/// Upstream `test_torrent_t` table.
const TEST_TORRENTS: &[(&str, &str, usize)] = &[
    // shared file in test2/test3 is not padded/aligned
    ("test2", "test1_pad_files", 0),
    ("test3", "test1_pad_files", 0),
    // test1's shared file is first (padded) but its tail isn't
    ("test1", "test1_pad_files", 0),
    ("test2", "test1_pad_files", 0),
    ("test3", "test1_pad_files", 0),
    ("test2", "test1_single", 0),
    // all padded; the small first file also accidentally matches
    ("test2_pad_files", "test1_pad_files", 2),
    ("test3_pad_files", "test1_pad_files", 2),
    ("test3_pad_files", "test2_pad_files", 2),
    ("test1_pad_files", "test2_pad_files", 2),
    ("test1_pad_files", "test3_pad_files", 2),
    ("test2_pad_files", "test3_pad_files", 2),
    // unpadded tail of the single-file torrent → last piece hash differs
    ("test1_pad_files", "test1_single", 0),
    // padded variant works
    ("test1_pad_files", "test1_single_padded", 1),
];

#[test]
fn resolve_links() {
    let Some(dir) = fixture_dir() else {
        eprintln!("skipping: no mutable_test_torrents fixture dir");
        return;
    };

    for (name1, name2, expected) in TEST_TORRENTS {
        let atp1 = load_torrent(
            &std::fs::read(dir.join(format!("{name1}.torrent"))).expect("fixture readable"),
        )
        .unwrap_or_else(|e| panic!("load {name1}: {e}"));
        let atp2 = load_torrent(
            &std::fs::read(dir.join(format!("{name2}.torrent"))).expect("fixture readable"),
        )
        .unwrap_or_else(|e| panic!("load {name2}: {e}"));

        let ti1: Arc<_> = atp1.ti.clone().expect("ti1");
        let ti2 = atp2.ti.expect("ti2");

        let rf = RenamedFiles::default();

        let mut links = ResolveLinks::new(ti1);
        links.matches(&ti2, ti2.files(), &rf, b".");

        let num_matches = links.get_links().iter().filter(|l| !l.is_empty()).count();
        assert_eq!(
            num_matches, *expected,
            "{name1} vs {name2}: {num_matches} links, expected {expected}"
        );
    }
}
