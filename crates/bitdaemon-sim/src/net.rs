//! Port of `test/simulation/` + `deps/libsimulator` (delivery queue,
//! per-link filters).
//!
//! `sim::simulation` core — registered endpoints + queued deliveries
//! ordered by virtual send/recv times. Upstream models per-link
//! latency/jitter/loss via `sim::filter`; we attach a `Link` config to
//! every (src → dst) pair and draw drops from a seeded splitmix64.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Per-link characteristics (`sim::filter` equivalent).
#[derive(Debug, Clone)]
pub struct Link {
    /// one-way added latency.
    pub latency: Duration,
    /// extra uniform jitter bound (ms).
    pub jitter_ms: u64,
    /// drop probability, 0.0..=1.0.
    pub loss: f64,
}

impl Default for Link {
    fn default() -> Self {
        Self {
            latency: Duration::from_millis(0),
            jitter_ms: 0,
            loss: 0.0,
        }
    }
}

/// One pending datagram delivery.
#[derive(Debug)]
pub struct Delivery {
    /// virtual tick the packet arrives at.
    pub at: u64,
    /// destination socket.
    pub to: SocketAddr,
    /// sender.
    pub from: SocketAddr,
    /// payload.
    pub data: Vec<u8>,
}

/// Per-endpoint inbound mailbox.
#[derive(Debug, Default)]
pub struct Mailbox {
    /// queued `(from, payload)` pairs.
    pub queue: std::collections::VecDeque<(SocketAddr, Vec<u8>)>,
    /// read waker.
    pub waker: Option<std::task::Waker>,
}

/// `sim::simulation` — the network graph. Endpoints register a
/// [`crate::SimSocket`]; `send` schedules a `Delivery` at
/// `now + latency`, `step` pushes due packets into mailboxes.
/// Deterministic: same seed + same sends → same delivery order.
#[derive(Debug)]
pub struct SimNet {
    /// `now` in virtual ms.
    pub now: u64,
    /// endpoint → mailbox.
    pub mailboxes: BTreeMap<SocketAddr, Arc<Mutex<Mailbox>>>,
    /// pending deliveries (ordered by `at`).
    queue: Vec<Delivery>,
    /// default link config.
    pub default_link: Link,
    /// per (from,to) overrides.
    links: BTreeMap<(SocketAddr, SocketAddr), Link>,
    /// splitmix64 stream for jitter/loss decisions.
    rng: u64,
}

impl Default for SimNet {
    fn default() -> Self {
        Self::new(0x5EED)
    }
}

fn splitmix64(s: &mut u64) -> u64 {
    *s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *s;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl SimNet {
    /// create with a deterministic RNG `seed`.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            now: 0,
            mailboxes: BTreeMap::new(),
            queue: Vec::new(),
            default_link: Link::default(),
            links: BTreeMap::new(),
            rng: seed,
        }
    }

    /// register a socket endpoint.
    pub fn register(&mut self, addr: SocketAddr) -> Arc<Mutex<Mailbox>> {
        self.mailboxes
            .entry(addr)
            .or_insert_with(|| Arc::new(Mutex::new(Mailbox::default())))
            .clone()
    }

    /// override the link `from → to` (uni-directional, like upstream).
    pub fn set_link(&mut self, from: SocketAddr, to: SocketAddr, link: Link) {
        self.links.insert((from, to), link);
    }

    /// packets in flight waiting for delivery.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.queue.len()
    }

    /// schedule a datagram; may drop it per link `loss`.
    pub fn send(&mut self, from: SocketAddr, to: SocketAddr, data: Vec<u8>) {
        let link = self.links.get(&(from, to)).unwrap_or(&self.default_link);
        let r = splitmix64(&mut self.rng);
        if link.loss > 0.0 && (r as f64 / u64::MAX as f64) < link.loss {
            return; // dropped
        }
        let jitter = if link.jitter_ms > 0 {
            r % (link.jitter_ms + 1)
        } else {
            0
        };
        let at = self.now + link.latency.as_millis() as u64 + jitter;
        self.queue.push(Delivery { at, to, from, data });
        self.queue.sort_by_key(|d| d.at);
    }

    /// deliver every packet due at `self.now`; wakes the sockets.
    /// Returns delivered count.
    pub fn step(&mut self) -> usize {
        let mut delivered = 0;
        let mut i = 0;
        while i < self.queue.len() {
            if self.queue[i].at <= self.now {
                let d = self.queue.remove(i);
                if let Some(mb) = self.mailboxes.get(&d.to) {
                    let mut g = mb.lock().unwrap_or_else(|e| e.into_inner());
                    g.queue.push_back((d.from, d.data));
                    if let Some(w) = g.waker.take() {
                        w.wake();
                    }
                }
                delivered += 1;
            } else {
                i += 1;
            }
        }
        delivered
    }

    /// advance virtual time to `t_ms` and flush all packets due.
    /// Returns total delivered.
    pub fn run_to(&mut self, t_ms: u64) -> usize {
        self.now = t_ms;
        self.step()
    }

    /// run until the queue is empty or `max_ms` virtual ms elapsed.
    /// Returns (virtual_time, delivered).
    pub fn drain(&mut self, max_ms: u64) -> (u64, usize) {
        let mut total = 0;
        while let Some(next) = self.next_event_in() {
            if self.now + next > max_ms {
                break;
            }
            self.now += next;
            total += self.step();
        }
        (self.now, total)
    }

    /// virtual ms until the next pending delivery.
    #[must_use]
    pub fn next_event_in(&self) -> Option<u64> {
        self.queue
            .iter()
            .map(|d| d.at.saturating_sub(self.now))
            .min()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_and_order() {
        let mut net = SimNet::new(1);
        let a: SocketAddr = "10.0.0.1:1".parse().unwrap_or_else(|_| unreachable!());
        let b: SocketAddr = "10.0.0.2:2".parse().unwrap_or_else(|_| unreachable!());
        net.register(a);
        net.register(b);
        net.set_link(
            a,
            b,
            Link {
                latency: Duration::from_millis(50),
                jitter_ms: 0,
                loss: 0.0,
            },
        );
        net.send(a, b, b"x".to_vec());
        assert_eq!(net.step(), 0); // not yet due
        assert_eq!(net.run_to(50), 1); // delivered when clock reaches at
    }

    #[test]
    fn loss_is_deterministic() {
        let run = || {
            let mut net = SimNet::new(42);
            let a: SocketAddr = "10.0.0.1:1".parse().unwrap_or_else(|_| unreachable!());
            let b: SocketAddr = "10.0.0.2:2".parse().unwrap_or_else(|_| unreachable!());
            net.register(b);
            net.set_link(
                a,
                b,
                Link {
                    latency: Duration::ZERO,
                    jitter_ms: 0,
                    loss: 0.5,
                },
            );
            for i in 0..100 {
                net.send(a, b, vec![i]);
            }
            net.drain(1000).1
        };
        assert_eq!(run(), run()); // same seed → same delivered count
    }
}
