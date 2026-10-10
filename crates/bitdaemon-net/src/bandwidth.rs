//! Port of `aux_/bandwidth_limit` / `aux_/bandwidth_queue_entry` /
//! `aux_/bandwidth_manager` — the token-bucket rate limiter.
//!
//! Upstream stores raw `bandwidth_channel*` in each `bw_request` and a
//! `shared_ptr<bandwidth_socket>` for the peer. The Rust version uses
//! `Rc<BandwidthChannel>` (`Cell` interior mutability — the channels
//! outlive the managers) and `Rc<dyn BandwidthSocket>`.

use std::cell::Cell;
use std::collections::VecDeque;
use std::rc::Rc;

/// `inf` — "unlimited" quota sentinel (`INT_MAX` upstream).
pub const INF: i64 = i32::MAX as i64;

/// `bandwidth_channel` — a token bucket.
#[derive(Debug, Default)]
pub struct BandwidthChannel {
    /// `tmp` — priority sum, temporary storage during distribution.
    pub tmp: Cell<i64>,
    /// `distribute_quota` — bytes to distribute this round.
    pub distribute_quota: Cell<i64>,
    /// `m_quota_left` — assigned-but-unused bytes (i64 upstream).
    quota_left: Cell<i64>,
    /// `m_limit` — bytes/sec (0 = infinite).
    limit: Cell<i64>,
}

impl BandwidthChannel {
    /// `bandwidth_channel()`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `throttle(limit)` — 0 means infinite.
    pub fn throttle(&self, limit: i64) {
        debug_assert!(limit >= 0);
        debug_assert!(limit < INF);
        self.limit.set(limit);
    }

    /// `throttle()`.
    #[must_use]
    pub fn throttle_value(&self) -> i64 {
        self.limit.get()
    }

    /// `quota_left()`.
    #[must_use]
    pub fn quota_left(&self) -> i64 {
        if self.limit.get() == 0 {
            return INF;
        }
        self.quota_left.get().max(0)
    }

    /// `update_quota(dt_milliseconds)` — refill the bucket; the cap is
    /// `3 * limit` (3 seconds of burst, upstream `m_quota_left / 3 >
    /// m_limit → clamp`) and `inf`.
    pub fn update_quota(&self, dt_milliseconds: i64) {
        debug_assert!(self.limit.get() >= 0);
        debug_assert!(self.limit.get() < INF);

        if self.limit.get() == 0 {
            return;
        }

        let to_add = (self.limit.get() * dt_milliseconds + 500) / 1000;
        let mut q = self.quota_left.get();
        if to_add > INF - q {
            q = INF;
        } else {
            q += to_add;
            if q / 3 > self.limit.get() {
                q = self.limit.get() * 3;
            }
            q = q.min(INF);
        }
        self.quota_left.set(q);
        self.distribute_quota.set(q.max(0));
    }

    /// `return_quota(amount)` — a disconnecting peer returns its
    /// unused quota.
    pub fn return_quota(&self, amount: i64) {
        debug_assert!(amount >= 0);
        if self.limit.get() == 0 {
            return;
        }
        self.quota_left.set(self.quota_left.get() + amount);
    }

    /// `use_quota(amount)`.
    pub fn use_quota(&self, amount: i64) {
        debug_assert!(amount >= 0);
        if self.limit.get() == 0 {
            return;
        }
        self.quota_left.set(self.quota_left.get() - amount);
    }

    /// `need_queueing(amount)` — optimization: if more than one second
    /// of quota remains, grant immediately instead of queueing.
    #[must_use]
    pub fn need_queueing(&self, amount: i64) -> bool {
        if self.quota_left.get() - amount < self.limit.get() {
            return true;
        }
        self.quota_left.set(self.quota_left.get() - amount);
        false
    }
}

/// `bandwidth_socket` — the interface a peer connection implements to
/// receive assigned quota.
pub trait BandwidthSocket {
    /// Called when the request is fully (or finally) assigned;
    /// `channels` is the manager's channel id (`upload`/`download`).
    fn assign_bandwidth(&self, channel: i32, amount: i64);

    /// `is_disconnecting()` — queued requests of dead peers are
    /// dropped and their quota returned.
    fn is_disconnecting(&self) -> bool;
}

/// `bw_request` — one queued bandwidth request.
pub struct BwRequest {
    /// `peer`.
    pub peer: Rc<dyn BandwidthSocket>,
    /// `priority` — 1 is normal.
    pub priority: i64,
    /// `assigned` — bytes assigned so far.
    pub assigned: i64,
    /// `request_size` — dispatch when `assigned` reaches this.
    pub request_size: i64,
    /// `ttl` — rounds this request survives (starts at 20 upstream).
    pub ttl: i32,
    /// `channel[]` — up to `MAX_BANDWIDTH_CHANNELS`.
    pub channels: Vec<Rc<BandwidthChannel>>,
}

/// `bw_request::max_bandwidth_channels`.
pub const MAX_BANDWIDTH_CHANNELS: usize = 10;

impl BwRequest {
    /// `bw_request(peer, blk, prio)` — `ttl` starts at 20.
    #[must_use]
    pub fn new(peer: Rc<dyn BandwidthSocket>, blk: i64, prio: i64) -> Self {
        debug_assert!(prio > 0);
        Self {
            peer,
            priority: prio,
            assigned: 0,
            request_size: blk,
            ttl: 20,
            channels: Vec::new(),
        }
    }

    /// `assign_bandwidth()` — takes quota from the most limiting
    /// channel (`distribute_quota * priority / tmp`), applies it to
    /// all channels, returns bytes assigned this round.
    pub fn assign_bandwidth(&mut self) -> i64 {
        debug_assert!(self.assigned < self.request_size);
        let mut quota = self.request_size - self.assigned;
        debug_assert!(quota >= 0);
        self.ttl -= 1;
        if quota == 0 {
            return quota;
        }

        for ch in &self.channels {
            if ch.throttle_value() == 0 {
                continue;
            }
            if ch.tmp.get() == 0 {
                continue;
            }
            quota = (ch.distribute_quota.get() * self.priority / ch.tmp.get()).min(quota);
        }
        self.assigned += quota;
        for ch in &self.channels {
            ch.use_quota(quota);
        }
        debug_assert!(self.assigned <= self.request_size);
        quota
    }
}

/// `bandwidth_manager` — the request queue + quota distribution for
/// one direction (upload or download) at one scope (global / torrent).
pub struct BandwidthManager {
    /// `m_channel` — which channel id this manager dispatches.
    channel: i32,
    /// `m_queue`.
    queue: VecDeque<BwRequest>,
    /// `m_queued_bytes` — sum of unassigned request bytes.
    queued_bytes: i64,
    /// `m_abort`.
    abort: bool,
}

impl BandwidthManager {
    /// `bandwidth_manager(channel)`.
    #[must_use]
    pub fn new(channel: i32) -> Self {
        Self {
            channel,
            queue: VecDeque::new(),
            queued_bytes: 0,
            abort: false,
        }
    }

    /// `queue_size()`.
    #[must_use]
    pub fn queue_size(&self) -> usize {
        self.queue.len()
    }

    /// `queued_bytes()`.
    #[must_use]
    pub fn queued_bytes(&self) -> i64 {
        self.queued_bytes
    }

    /// `is_queued(peer)` — used by the upstream invariant check.
    #[must_use]
    pub fn is_queued(&self, peer: &dyn BandwidthSocket) -> bool {
        self.queue
            .iter()
            .any(|r| std::ptr::eq(r.peer.as_ref(), peer))
    }

    /// `close()` — `m_abort = true`.
    pub fn close(&mut self) {
        self.abort = true;
        self.queued_bytes = 0;
        self.queue.clear();
    }

    /// `request_bandwidth(peer, blk, priority, channels)` — 0 = the
    /// request was queued; `blk` = granted immediately.
    pub fn request_bandwidth(
        &mut self,
        peer: Rc<dyn BandwidthSocket>,
        blk: i64,
        priority: i64,
        channels: &[Rc<BandwidthChannel>],
    ) -> i64 {
        if self.abort {
            return 0;
        }

        debug_assert!(blk > 0);
        debug_assert!(priority > 0);
        debug_assert!(channels.len() <= MAX_BANDWIDTH_CHANNELS);
        debug_assert!(!self.is_queued(peer.as_ref()));

        if channels.is_empty() {
            // not rate limited — satisfy immediately
            return blk;
        }

        let mut bwr = BwRequest::new(peer, blk, priority);
        for c in channels {
            if c.need_queueing(blk) {
                bwr.channels.push(Rc::clone(c));
            }
        }

        if bwr.channels.is_empty() {
            return blk;
        }

        self.queued_bytes += blk;
        self.queue.push_back(bwr);
        0
    }

    /// `update_quotas(dt_milliseconds)` — one round of quota
    /// distribution (called every second upstream; `dt` capped at
    /// 3000 ms).
    pub fn update_quotas(&mut self, dt_milliseconds: i64) {
        if self.abort || self.queue.is_empty() {
            return;
        }

        let dt = dt_milliseconds.min(3000);
        let mut done: Vec<BwRequest> = Vec::new();

        // drop disconnecting peers, return their quota, reset tmp
        let mut i = 0;
        while i < self.queue.len() {
            if self.queue[i].peer.is_disconnecting() {
                let mut req = self.queue.remove(i).expect("in bounds");
                self.queued_bytes -= req.request_size - req.assigned;
                for ch in &req.channels {
                    ch.return_quota(req.assigned);
                }
                req.assigned = 0;
                done.push(req);
            } else {
                for ch in &self.queue[i].channels {
                    ch.tmp.set(0);
                }
                i += 1;
            }
        }

        // per-channel priority sums
        let mut channels: Vec<Rc<BandwidthChannel>> = Vec::new();
        for r in &self.queue {
            for ch in &r.channels {
                if ch.tmp.get() == 0 {
                    channels.push(Rc::clone(ch));
                }
                ch.tmp.set(ch.tmp.get() + r.priority);
            }
        }

        for ch in &channels {
            ch.update_quota(dt);
        }

        // assign; dispatch completed or expired requests
        let mut i = 0;
        while i < self.queue.len() {
            let mut a = self.queue[i].assign_bandwidth();
            if self.queue[i].assigned == self.queue[i].request_size
                || (self.queue[i].ttl <= 0 && self.queue[i].assigned > 0)
            {
                a += self.queue[i].request_size - self.queue[i].assigned;
                done.push(self.queue.remove(i).expect("in bounds"));
            } else {
                i += 1;
            }
            self.queued_bytes -= a;
        }

        // LIFO dispatch order (upstream pops from the back)
        while let Some(bwr) = done.pop() {
            bwr.peer.assign_bandwidth(self.channel, bwr.assigned);
        }
    }
}
