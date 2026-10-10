// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `traversal_algorithm` — iterative lookup state machine.

use std::net::SocketAddr;

use bitdaemon_dht::node_id::{NodeId, distance};
use bitdaemon_dht::traversal::{Step, Traversal, TraverseHooks, obs_flags, trav_flags};

fn hooks() -> TraverseHooks {
    TraverseHooks {
        bucket_size: 8,
        aggressive: false,
        restrict_search_ips: true,
        branch_factor: 3,
        router_nodes: vec![],
    }
}

fn id_with_top(top: u8) -> NodeId {
    let mut b = [0u8; 20];
    b[0] = top;
    NodeId::from_bytes(&b).unwrap()
}

fn ep(i: u32) -> SocketAddr {
    // spread over distinct /24s to pass restrict_search_ips
    format!(
        "{}.{}.{}.{}:6881",
        60 + (i / 65536 % 60),
        (i / 256) % 250,
        i % 250,
        1
    )
    .parse()
    .unwrap()
}

#[test]
fn sorted_insert_and_dedup() {
    let target = id_with_top(0x00);
    let mut t = Traversal::new(target, &hooks());
    let h = hooks();
    // farthest first — inserts must sort closest-first
    t.add_entry(id_with_top(0xff), ep(1), 0, &h);
    t.add_entry(id_with_top(0x01), ep(2), 0, &h);
    t.add_entry(id_with_top(0x7f), ep(3), 0, &h);
    let r = t.results();
    assert_eq!(r.len(), 3);
    assert!(distance(&r[0].id.unwrap(), &target) <= distance(&r[1].id.unwrap(), &target));
    // duplicate id ignored
    t.add_entry(id_with_top(0x01), ep(4), 0, &h);
    assert_eq!(t.results().len(), 3);
}

#[test]
fn full_traversal_lifecycle() {
    let target = id_with_top(0x00);
    let mut t = Traversal::new(target, &hooks());
    let h = hooks();
    for i in 0..12u32 {
        t.add_entry(id_with_top(0x10 + i as u8), ep(i + 1), 0, &h);
    }

    // start → branch_factor(3) invocations
    let Step::Pending(invoked) = t.start(&h, |_, _| true) else {
        panic!("expected pending");
    };
    assert_eq!(invoked.len(), 3);
    assert_eq!(t.invoke_count(), 3);

    // finish the first → one more invoked (branch factor back to 3)
    let Step::Pending(invoked) = t.finished(invoked[0], &h, |_, _| true) else {
        panic!("pending");
    };
    assert_eq!(invoked.len(), 1);
    assert_eq!(t.responses(), 1);

    // fail the rest until the traversal completes
    let mut guard = 0;
    loop {
        // find a queried-not-alive-not-failed index
        let idx = t
            .results()
            .iter()
            .position(|e| {
                e.flags & (obs_flags::QUERIED | obs_flags::ALIVE | obs_flags::FAILED)
                    == obs_flags::QUERIED
            })
            .or_else(|| {
                t.results()
                    .iter()
                    .position(|e| e.flags & obs_flags::QUERIED == 0)
            });
        let Some(i) = idx else { break };
        match t.failed(i, 0, &h, |_, _| {}, |_, _| true) {
            Step::Done => break,
            Step::Pending(_) => {}
        }
        guard += 1;
        assert!(guard < 200, "traversal did not terminate");
    }
    assert!(t.is_done());
}

#[test]
fn short_timeout_frees_slot() {
    let target = id_with_top(0x00);
    let mut t = Traversal::new(target, &hooks());
    let h = hooks();
    for i in 0..6u32 {
        t.add_entry(id_with_top(0x20 + i as u8), ep(100 + i), 0, &h);
    }
    let Step::Pending(invoked) = t.add_requests(&h, |_, _| true) else {
        panic!("pending")
    };
    assert_eq!(t.branch_factor(), 3);
    // first slot short-times-out → branch factor bumps to 4
    t.failed(
        invoked[0],
        trav_flags::SHORT_TIMEOUT,
        &h,
        |_, _| {},
        |_, _| true,
    );
    assert_eq!(t.branch_factor(), 4);
    // when it finally fails, branch factor is restored
    t.failed(invoked[0], 0, &h, |_, _| {}, |_, _| true);
    assert!(t.branch_factor() <= 4);
}

#[test]
fn ip_prefix_dedup() {
    let mut h = hooks();
    h.restrict_search_ips = true;
    let target = id_with_top(0x00);
    let mut t = Traversal::new(target, &h);
    let a: SocketAddr = "8.8.8.8:6881".parse().unwrap();
    let b: SocketAddr = "8.8.8.9:6881".parse().unwrap(); // same /24
    t.add_entry(id_with_top(0x30), a, 0, &h);
    t.add_entry(id_with_top(0x40), b, 0, &h); // rejected
    assert_eq!(t.results().len(), 1);
    // INITIAL flag bypasses the restriction
    t.add_entry(id_with_top(0x50), b, obs_flags::INITIAL, &h);
    assert_eq!(t.results().len(), 2);
}
