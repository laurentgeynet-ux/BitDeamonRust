//! Phase F benchmark — peer-wire encode/decode throughput baseline.

use criterion::{Criterion, black_box, criterion_group, criterion_main};

use bitdaemon_net::peer_wire::{self, PeerMessage};

fn bench_encode(c: &mut Criterion) {
    let msg = PeerMessage::Piece {
        piece: bitdaemon_common::types::PieceIndex(17),
        offset: 0,
        data: bytes::Bytes::from(vec![0xABu8; 16 * 1024]),
    };
    c.bench_function("peer_wire/encode-16k-piece", |b| {
        b.iter(|| peer_wire::encode(black_box(&msg)));
    });
}

fn bench_decode(c: &mut Criterion) {
    let msg = PeerMessage::Have(bitdaemon_common::types::PieceIndex(1234));
    let buf = peer_wire::encode(&msg);
    c.bench_function("peer_wire/decode-have", |b| {
        b.iter(|| peer_wire::decode(black_box(&buf)));
    });
}

criterion_group!(benches, bench_encode, bench_decode);
criterion_main!(benches);
