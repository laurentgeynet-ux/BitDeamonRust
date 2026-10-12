//! Deterministic mutation driver for the three decode surfaces that the
//! libFuzzer targets exercise (`bdecode_checked`, `peer_wire`,
//! `read_resume_data`). Used where cargo-fuzz cannot link (Windows MSVC
//! lacks sancov sections, no mingw toolchain): same panic-freedom
//! contract, structured mutations over a valid corpus, panic tally via
//! `catch_unwind`.
//!
//! Usage: `mutation_driver [seconds]` — default 120.

use std::panic;
use std::time::{Duration, Instant};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // splitmix64
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn range(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }
}

fn corpus() -> Vec<Vec<u8>> {
    let mut c: Vec<Vec<u8>> = vec![
        // bencode shapes
        b"d3:bar4:spam3:fooi42ee".to_vec(),
        b"d4:infod4:name4:test12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaaee".to_vec(),
        b"l4:spam4:eggsi42ed1:ai1eee".to_vec(),
        b"i-9223372036854775808e".to_vec(),
        b"0:".to_vec(),
        b"d1:ai1e1:bi-1e1:cld1:xleeee".to_vec(),
        // resume-data shape (subset of upstream keys)
        b"d6:pieces4:\xff\xff\xff\xff8:save_path4:/tmp4:name4:teste".to_vec(),
        b"d6:pieces0:4:infod4:name4:test12:piece lengthi16384e6:pieces20:bbbbbbbbbbbbbbbbbbbbeee"
            .to_vec(),
        // wire: valid handshake
        {
            let mut h = vec![19u8];
            h.extend_from_slice(b"BitTorrent protocol");
            h.extend_from_slice(&[0; 8]);
            h.extend_from_slice(&[0xAA; 20]);
            h.extend_from_slice(b"-BD0001-zzzzzzzzzzzz");
            h
        },
        // wire messages: interested / choke / have / request / piece / bitfield
        vec![0, 0, 0, 1, 2],
        vec![0, 0, 0, 1, 0],
        vec![0, 0, 0, 5, 4, 0, 0, 0, 7],
        vec![0, 0, 0, 17, 6, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0x40, 0],
        vec![0, 0, 0, 9, 5, 0xff],
        // garbage edges
        vec![],
        vec![0xff; 64],
        b"999999999999999999999999".to_vec(),
        b"llllddddeeee".to_vec(),
    ];
    if let Ok(t) = std::fs::read("../banc_test/mse.torrent") {
        c.push(t);
    }
    if let Ok(r) = std::fs::read("../banc_test/test.session") {
        c.push(r);
    }
    c
}

fn mutate(rng: &mut Rng, seed: &[u8], corpus: &[Vec<u8>]) -> Vec<u8> {
    let mut v = seed.to_vec();
    for _ in 0..1 + rng.range(6) {
        match rng.range(7) {
            0 if !v.is_empty() => {
                let i = rng.range(v.len());
                v[i] ^= 1 << rng.range(8);
            }
            1 => {
                let i = rng.range(v.len() + 1);
                v.insert(i, rng.next() as u8);
            }
            2 if !v.is_empty() => {
                let i = rng.range(v.len());
                let n = 1 + rng.range(8.min(v.len() - i));
                v.drain(i..i + n);
            }
            3 if !v.is_empty() => {
                let i = rng.range(v.len());
                let n = 1 + rng.range(16.min(v.len() - i));
                let chunk = v[i..i + n].to_vec();
                let j = rng.range(v.len() + 1);
                v.splice(j..j, chunk);
            }
            4 => {
                let i = rng.range(v.len() + 1);
                let other = &corpus[rng.range(corpus.len())];
                if !other.is_empty() {
                    let s = rng.range(other.len());
                    let n = 1 + rng.range(other.len() - s);
                    let chunk = other[s..s + n].to_vec();
                    v.splice(i..i, chunk);
                }
            }
            5 => {
                let tokens: &[&[u8]] =
                    &[b"d", b"e", b"i", b"l", b":", b"999999999", b"i-1e", b"03:x"];
                let t = tokens[rng.range(tokens.len())];
                let i = rng.range(v.len() + 1);
                v.splice(i..i, t.iter().copied());
            }
            _ if v.len() > 1 => {
                v.truncate(rng.range(v.len()));
            }
            _ => {}
        }
    }
    if v.len() > 1 << 20 {
        v.truncate(1 << 20);
    }
    v
}

fn harness(data: &[u8]) {
    if let Ok(n) = bitdaemon_bencode::bdecode_checked(data) {
        let _ = bitdaemon_bencode::print_node(&n, true, 0);
    }
    use bitdaemon_net::peer_wire;
    if let Ok(hs) = peer_wire::parse_handshake(data) {
        let _ = peer_wire::write_handshake(
            &hs.info_hash,
            &hs.peer_id,
            hs.supports_dht(),
            hs.supports_extension(),
            hs.supports_fast(),
            hs.supports_v2(),
        );
    }
    if data.len() >= 4 {
        let len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() >= 4 + len {
            let _ = peer_wire::decode(&data[4..4 + len]);
        }
    }
    let limits = bitdaemon_core::torrent_info::LoadTorrentLimits::default();
    if let Ok(atp) = bitdaemon_core::resume_data::read_resume_data(data, &limits) {
        let _ = bitdaemon_core::resume_data::write_resume_data_buf(&atp);
    }
}

fn main() {
    let secs = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);
    panic::set_hook(Box::new(|_| {}));
    let corpus = corpus();
    let mut rng = Rng(0xBD5E_ED01_2345_6789);
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut iters = 0u64;
    let mut panics = 0u64;
    while Instant::now() < deadline {
        let seed = &corpus[rng.range(corpus.len())];
        let input = mutate(&mut rng, seed, &corpus);
        iters += 1;
        if panic::catch_unwind(|| harness(&input)).is_err() {
            panics += 1;
            let name = format!("crash-{iters}.bin");
            if std::fs::write(&name, &input).is_ok() {
                eprintln!("panic -> saved {name}");
            }
        }
    }
    println!("done: {iters} inputs, {panics} panics");
}
