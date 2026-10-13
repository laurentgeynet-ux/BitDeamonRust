//! Phase F benchmark — bdecode/encode throughput baseline for the
//! upstream-vs-Rust comparison (ADR-0006).

use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn sample_torrent_info() -> Vec<u8> {
    // small nested dict approximating a .torrent info dictionary
    let mut v = Vec::new();
    v.extend_from_slice(
        b"d4:infod6:lengthi10485760e4:name8:ubuntu.is12:piece lengthi262144e6:pieces20:",
    );
    v.extend_from_slice(&[0xAAu8; 20]);
    v.extend_from_slice(b"ee");
    v
}

fn bench_decode(c: &mut Criterion) {
    let data = sample_torrent_info();
    c.bench_function("bdecode/info-dict", |b| {
        b.iter(|| {
            let n = bitdaemon_bencode::bdecode(black_box(&data));
            let _ = n.dict_find(b"info");
        });
    });
}

fn bench_encode(c: &mut Criterion) {
    let mut e = bitdaemon_bencode::Entry::Dict(Default::default());
    let info = e.entry_mut(b"info").expect("dict");
    *info = bitdaemon_bencode::Entry::Dict(Default::default());
    *info.entry_mut(b"length").expect("dict") = 10_485_760i64.into();
    *info.entry_mut(b"name").expect("dict") = "ubuntu.iso".into();
    c.bench_function("bencode/entry", |b| {
        b.iter(|| bitdaemon_bencode::bencode(black_box(&e)));
    });
}

criterion_group!(benches, bench_decode, bench_encode);
criterion_main!(benches);
