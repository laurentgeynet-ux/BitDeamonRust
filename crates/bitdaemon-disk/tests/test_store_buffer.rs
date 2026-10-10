//! Tests for `StoreBuffer` — port of `aux::store_buffer` semantics.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bitdaemon_common::types::{PieceIndex, StorageIndex};
use bitdaemon_disk::back_pressure::DiskObserver;
use bitdaemon_disk::store_buffer::{StoreBuffer, TorrentLocation};

fn loc(t: u32, p: i32, o: i32) -> TorrentLocation {
    TorrentLocation {
        torrent: StorageIndex(t),
        piece: PieceIndex(p),
        offset: o,
    }
}

#[test]
fn insert_get_erase() {
    // direct poster — callbacks run inline
    let sb = StoreBuffer::new(Arc::new(|cbs| {
        for w in cbs {
            if let Some(o) = w.upgrade() {
                o.on_disk();
            }
        }
    }));
    sb.set_max_size(10);

    let buf: Arc<[u8]> = vec![1u8, 2, 3].into();
    sb.insert(loc(0, 0, 0), buf.clone(), None);

    assert_eq!(sb.len(), 1);
    assert_eq!(sb.get(loc(0, 0, 0), |b| b.to_vec()), Some(vec![1, 2, 3]));
    assert_eq!(sb.get(loc(0, 0, 1), |b| b.to_vec()), None);

    sb.erase(loc(0, 0, 0));
    assert!(sb.is_empty());
    assert_eq!(sb.get(loc(0, 0, 0), |b| b.to_vec()), None);
}

#[test]
fn get2_both_and_one() {
    let sb = StoreBuffer::new(Arc::new(|_| {}));
    sb.set_max_size(100);

    let a: Arc<[u8]> = vec![7u8; 4].into();
    sb.insert(loc(0, 1, 0), a, None);

    // both miss → None (upstream returns 0)
    assert!(sb.get2(loc(0, 0, 0), loc(0, 0, 1), |_, _| ()).is_none());

    // one hit
    let r = sb.get2(loc(0, 1, 0), loc(0, 0, 1), |b1, b2| {
        (b1.map(<[u8]>::to_vec), b2.map(<[u8]>::to_vec))
    });
    assert_eq!(r, Some((Some(vec![7u8; 4]), None)));
}

struct Counting {
    count: AtomicUsize,
}

impl DiskObserver for Counting {
    fn on_disk(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn back_pressure_release_notifies() {
    let released = Arc::new(Counting {
        count: AtomicUsize::new(0),
    });
    let sb = StoreBuffer::new(Arc::new(|cbs| {
        for w in cbs {
            if let Some(o) = w.upgrade() {
                o.on_disk();
            }
        }
    }));
    // back-pressure trips at level >= max_size; low watermark = 8*3/4 = 6
    sb.set_max_size(8);

    let buf: Arc<[u8]> = vec![0u8; 1].into();
    let mut pressurized = false;
    for i in 0..8 {
        pressurized |= sb.insert(loc(0, i, 0), buf.clone(), Some(released.clone()));
    }
    assert!(pressurized, "max_size should report back-pressure");

    // drain below the low watermark → observer released
    for i in 0..6 {
        sb.erase(loc(0, i, 0));
    }
    assert!(released.count.load(Ordering::Relaxed) >= 1);
}

#[test]
fn erase_missing_is_noop_release() {
    let sb = StoreBuffer::new(Arc::new(|_| {}));
    sb.set_max_size(4);
    let buf: Arc<[u8]> = vec![5u8; 2].into();
    sb.insert(loc(1, 2, 0), buf, None);
    sb.erase(loc(1, 2, 0));
    assert!(sb.is_empty());
}
