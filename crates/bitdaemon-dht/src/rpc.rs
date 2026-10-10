// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `kademlia/rpc_manager.cpp` — transaction bookkeeping
//! and reply/timeout dispatch for DHT queries.
//!
//! Upstream keeps `observer` shared_ptrs in `m_transactions`; here
//! each pending request carries an opaque `tag` and a
//! [`RpcHandler`] the caller supplies per request — the wiring to
//! `Traversal`/observer state lives in the session layer, keeping
//! this manager free of borrow cycles.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bitdaemon_bencode::Entry;

use crate::node_id::NodeId;

/// `seconds(1)` upstream.
pub const SHORT_TIMEOUT: Duration = Duration::from_secs(1);
/// `seconds(15)` upstream.
pub const TIMEOUT: Duration = Duration::from_secs(15);

/// What [`RpcManager::tick`] reports per expired request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expiry {
    /// > [`SHORT_TIMEOUT`] without reply (frees a branch slot).
    ShortTimeout,
    /// > [`TIMEOUT`] — final failure.
    Timeout,
}

/// One in-flight query (an `observer` slot in `m_transactions`).
struct Pending {
    ep: SocketAddr,
    sent: Instant,
    short_timed_out: bool,
    /// caller-defined dispatch payload
    tag: u64,
}

/// `rpc_manager` — transaction-id → pending request map.
///
/// Generic over the per-request dispatch tag (`u64` — e.g. a
/// traversal index) chosen by the caller at [`Self::add`].
#[derive(Default)]
pub struct RpcManager {
    /// `m_transactions` (multimap tid → pending).
    transactions: HashMap<u16, Vec<Pending>>,
}

/// Outcome of [`RpcManager::incoming`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Incoming {
    /// matched a pending request — `r.id` is the responder's
    /// verified node id (caller may `node_seen` it).
    Reply { tag: u64, id: NodeId },
    /// the peer answered with a `y:"e"` error, or the reply was
    /// malformed — treat as a timeout (`observer::timeout()`).
    Error { tag: u64 },
    /// no matching transaction — ignore (don't answer: would be
    /// a reflection-amplification vector).
    Ignored,
}

impl RpcManager {
    /// `rpc_manager(our_id, settings, table, …)`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `invoke()` — build the query dict (`y:"q"`, `a.id`,
    /// `t: tid`, `ro`, `want`) and register the transaction.
    /// `send` pushes the encoded packet out (injected transport).
    /// Returns `Some(tid)` when the send succeeded.
    pub fn invoke(
        &mut self,
        ep: SocketAddr,
        tag: u64,
        our_id: NodeId,
        read_only: bool,
        want_v6: bool,
        send: impl FnOnce(Entry, SocketAddr) -> bool,
    ) -> Option<u16> {
        let tid: u16 = rand::random();
        let mut q = Entry::Dict(Default::default());
        let e = q.dict_mut().expect("fresh dict");
        e.insert("y".into(), Entry::String(b"q".to_vec()));
        e.insert("t".into(), Entry::String(tid.to_be_bytes().to_vec()));
        let mut a = Entry::Dict(Default::default());
        let am = a.dict_mut().expect("fresh dict");
        am.insert("id".into(), Entry::String(our_id.as_bytes().to_vec()));
        if want_v6 {
            am.insert(
                "want".into(),
                Entry::List(vec![Entry::String(b"n6".to_vec())]),
            );
        }
        e.insert("a".into(), a);
        if read_only {
            e.insert("ro".into(), Entry::Int(1));
        }
        if !send(q, ep) {
            return None;
        }
        self.transactions.entry(tid).or_default().push(Pending {
            ep,
            sent: Instant::now(),
            short_timed_out: false,
            tag,
        });
        Some(tid)
    }

    /// `incoming(msg, &mut id)` — match `t` + source address to a
    /// pending transaction. `resp` is the `r` dict (already
    /// decoded); `is_error` reflects `y == "e"`.
    /// `enforce_id` is `dht_enforce_node_id` (BEP-42).
    pub fn incoming(
        &mut self,
        from: SocketAddr,
        tid_bytes: &[u8],
        is_error: bool,
        resp_id: Option<NodeId>,
        enforce_id: bool,
    ) -> Incoming {
        if tid_bytes.len() != 2 {
            return Incoming::Ignored;
        }
        let tid = u16::from_be_bytes([tid_bytes[0], tid_bytes[1]]);
        let Some(list) = self.transactions.get_mut(&tid) else {
            return Incoming::Ignored;
        };
        let Some(pos) = list.iter().position(|p| p.ep == from) else {
            if list.is_empty() {
                self.transactions.remove(&tid);
            }
            return Incoming::Ignored;
        };
        let pending = list.remove(pos);
        if list.is_empty() {
            self.transactions.remove(&tid);
        }

        if is_error {
            return Incoming::Error { tag: pending.tag };
        }
        match resp_id {
            Some(id) if !enforce_id || crate::node_id::verify_id(&id, &from.ip()) => {
                Incoming::Reply {
                    tag: pending.tag,
                    id,
                }
            }
            _ => Incoming::Error { tag: pending.tag },
        }
    }

    /// `tick()` — fire short/final timeouts; returns how long the
    /// caller may sleep before the next tick (clamped to 200 ms
    /// minimum cadence upstream).
    pub fn tick(&mut self, mut on_expired: impl FnMut(u64, SocketAddr, Expiry)) -> Duration {
        if self.transactions.is_empty() {
            return SHORT_TIMEOUT;
        }
        let now = Instant::now();
        let mut expired = Vec::new();
        let mut ret = SHORT_TIMEOUT;
        for (tid, list) in self.transactions.iter_mut() {
            let tid = *tid;
            list.retain(|p| {
                let diff = now.duration_since(p.sent);
                if diff >= TIMEOUT {
                    expired.push((p.tag, p.ep, Expiry::Timeout));
                    false
                } else {
                    if diff >= SHORT_TIMEOUT && !p.short_timed_out {
                        expired.push((p.tag, p.ep, Expiry::ShortTimeout));
                        // caller marks the observer — on next tick the
                        // flag skips re-fire
                        let _ = tid;
                    }
                    ret = ret.min(TIMEOUT - diff);
                    true
                }
            });
            for p in list.iter_mut() {
                let diff = now.duration_since(p.sent);
                if diff >= SHORT_TIMEOUT && !p.short_timed_out {
                    p.short_timed_out = true;
                }
            }
        }
        self.transactions.retain(|_, l| !l.is_empty());
        for (tag, ep, ex) in expired {
            on_expired(tag, ep, ex);
        }
        ret.max(Duration::from_millis(200))
    }

    /// `allocated_observers()` — in-flight requests.
    #[must_use]
    pub fn outstanding(&self) -> usize {
        self.transactions.values().map(Vec::len).sum()
    }

    /// `~rpc_manager` — drop all; callers clear their observers.
    pub fn abort(&mut self) {
        self.transactions.clear();
    }
}
