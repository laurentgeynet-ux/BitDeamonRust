//! Temporary diagnostic — measures raw positioned-write latency on a
//! sparse file with N concurrent writers, matching the real torrent
//! workload (scattered ~48 KiB flushes). Run:
//! `cargo test --release -p bitdaemon-disk --test test_win_bench -- --nocapture`

#![cfg(windows)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bitdaemon_disk::file::{FileHandle, pwrite_all};
use bitdaemon_disk::open_mode::OpenMode;

const FILE_LEN: u64 = 512 * 1024 * 1024;
const CHUNK: usize = 49 * 1024;
const THREADS: u64 = 10;
const WRITES_PER_THREAD: u64 = 300;

#[test]
fn sparse_pwrite_latency() {
    let dir = std::env::temp_dir().join("bd_pwrite_bench");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("bench_sparse.bin");
    let _ = std::fs::remove_file(&path);

    let handle = Arc::new(
        FileHandle::open(&path, 0, OpenMode::WRITE | OpenMode::SPARSE).expect("open sparse"),
    );

    let total_us = Arc::new(AtomicU64::new(0));
    let max_us = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let mut joins = Vec::new();
    for tid in 0..THREADS {
        let h = Arc::clone(&handle);
        let tu = Arc::clone(&total_us);
        let mu = Arc::clone(&max_us);
        joins.push(std::thread::spawn(move || {
            let buf = vec![0xabu8; CHUNK];
            let chunks = FILE_LEN / CHUNK as u64;
            let mut seed = tid.wrapping_mul(0x9e3779b97f4a7c15) ^ 0x12345;
            for _ in 0..WRITES_PER_THREAD {
                // xorshift64 — cheap deterministic scatter
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                let off = (seed % chunks) * CHUNK as u64;
                let t = Instant::now();
                pwrite_all(h.file(), &buf, off).expect("pwrite");
                let us = t.elapsed().as_micros() as u64;
                tu.fetch_add(us, Ordering::Relaxed);
                mu.fetch_max(us, Ordering::Relaxed);
            }
        }));
    }
    for j in joins {
        j.join().unwrap();
    }
    let wall = t0.elapsed();
    let n = THREADS * WRITES_PER_THREAD;
    let total = total_us.load(Ordering::Relaxed);
    let mib = (n * CHUNK as u64) as f64 / 1024.0 / 1024.0;
    eprintln!(
        "sparse:  n={n} avg={:.2}ms max={:.2}ms wall={:?} agg={:.1} MiB/s",
        total as f64 / 1000.0 / n as f64,
        max_us.load(Ordering::Relaxed) as f64 / 1000.0,
        wall,
        mib / wall.as_secs_f64(),
    );

    // same but non-sparse (allocate-style full file)
    let path2 = dir.join("bench_full.bin");
    let _ = std::fs::remove_file(&path2);
    let handle2 = Arc::new(
        FileHandle::open(
            &path2,
            FILE_LEN,
            OpenMode::WRITE | OpenMode::TRUNCATE | OpenMode::ALLOW_SET_FILE_VALID_DATA,
        )
        .expect("open"),
    );
    total_us.store(0, Ordering::Relaxed);
    max_us.store(0, Ordering::Relaxed);
    let t0 = Instant::now();
    let mut joins = Vec::new();
    for tid in 0..THREADS {
        let h = Arc::clone(&handle2);
        let tu = Arc::clone(&total_us);
        let mu = Arc::clone(&max_us);
        joins.push(std::thread::spawn(move || {
            let buf = vec![0xabu8; CHUNK];
            let chunks = FILE_LEN / CHUNK as u64;
            let mut seed = tid.wrapping_mul(0x9e3779b97f4a7c15) ^ 0x6789;
            for _ in 0..WRITES_PER_THREAD {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                let off = (seed % chunks) * CHUNK as u64;
                let t = Instant::now();
                pwrite_all(h.file(), &buf, off).expect("pwrite");
                let us = t.elapsed().as_micros() as u64;
                tu.fetch_add(us, Ordering::Relaxed);
                mu.fetch_max(us, Ordering::Relaxed);
            }
        }));
    }
    for j in joins {
        j.join().unwrap();
    }
    let wall = t0.elapsed();
    let n = THREADS * WRITES_PER_THREAD;
    let total = total_us.load(Ordering::Relaxed);
    let mib = (n * CHUNK as u64) as f64 / 1024.0 / 1024.0;
    eprintln!(
        "full:    n={n} avg={:.2}ms max={:.2}ms wall={:?} agg={:.1} MiB/s",
        total as f64 / 1000.0 / n as f64,
        max_us.load(Ordering::Relaxed) as f64 / 1000.0,
        wall,
        mib / wall.as_secs_f64(),
    );
}
