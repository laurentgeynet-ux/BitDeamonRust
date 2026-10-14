// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `kademlia/dht_storage.cpp` — the default DHT storage:
//! `announce_peer`/`get_peers` (incl. scrape bloom filters and
//! write-token gating), immutable/mutable items (BEP-44),
//! `sample_infohashes` (BEP-51), and `tick` expiry.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use bitdaemon_common::bloom_filter::BloomFilter;
use bitdaemon_common::hash::Sha1Hash;
use bitdaemon_common::random::random;
use bitdaemon_net::ip_helpers::{hash_address, same_v6_prefix};
use bitdaemon_net::socket_io::write_endpoint;

use crate::dht_settings::DhtSettings;
use crate::item::{PublicKey, SequenceNumber, Signature64};
use crate::node_id::{NodeId, min_distance_exp};

/// `announce_interval` — peers expire after 1.5 × 30 min.
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(30 * 60);
/// `max_peers_per_prefix` — entries one IPv6 /56 can claim per
/// info-hash.
const MAX_PEERS_PER_PREFIX: usize = 16;
/// `sample_infohashes_interval_max`.
pub const SAMPLE_INFOHASHES_INTERVAL_MAX: i32 = 21600;
/// `infohashes_sample_count_max`.
pub const INFOHASHES_SAMPLE_COUNT_MAX: i32 = 20;

/// `peer_entry`.
#[derive(Debug, Clone)]
pub struct PeerEntry {
    /// `added`.
    pub added: Instant,
    /// `addr`.
    pub addr: SocketAddr,
    /// `seed`.
    pub seed: bool,
}

/// `torrent_entry` — peers split by family.
#[derive(Debug, Default, Clone)]
pub struct TorrentEntry {
    /// `name` — optional torrent name (≤100 chars).
    pub name: String,
    /// `peers4`.
    pub peers4: Vec<PeerEntry>,
    /// `peers6`.
    pub peers6: Vec<PeerEntry>,
}

/// `dht_immutable_item`.
#[derive(Debug, Clone)]
pub struct DhtImmutableItem {
    /// `value` — bencoded payload.
    pub value: Vec<u8>,
    /// `ips` — announcer bloom (popularity estimate).
    pub ips: BloomFilter<128>,
    /// `last_seen`.
    pub last_seen: Instant,
    /// `num_announcers`.
    pub num_announcers: i32,
}

/// `dht_mutable_item`.
#[derive(Debug, Clone)]
pub struct DhtMutableItem {
    /// `value`.
    pub value: Vec<u8>,
    /// `ips`.
    pub ips: BloomFilter<128>,
    /// `last_seen`.
    pub last_seen: Instant,
    /// `num_announcers`.
    pub num_announcers: i32,
    /// `sig`.
    pub sig: Signature64,
    /// `seq`.
    pub seq: SequenceNumber,
    /// `key`.
    pub key: PublicKey,
    /// `salt`.
    pub salt: Vec<u8>,
}

/// `dht_storage_counters`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DhtStorageCounters {
    /// `torrents`.
    pub torrents: i32,
    /// `peers`.
    pub peers: i32,
    /// `immutable_data`.
    pub immutable_data: i32,
    /// `mutable_data`.
    pub mutable_data: i32,
}

/// `get_peers` reply material.
#[derive(Debug, Default)]
pub struct GetPeersReply {
    /// `n` — torrent name.
    pub name: Option<String>,
    /// `values` — compact endpoints (6 or 18 bytes each). `Some`
    /// when the info-hash is known and this isn't a scrape reply —
    /// upstream emits the key even when the list comes out empty.
    pub peers: Option<Vec<u8>>,
    /// `BFpe`/`BFsd` for scrape queries.
    pub bfpe: Option<Vec<u8>>,
    /// seed bloom.
    pub bfsd: Option<Vec<u8>>,
    /// `write_tok` gate result: `true` = don't issue a write token.
    pub no_token: bool,
}

/// `touch_item` — bump last_seen and count distinct announcers.
fn touch(ips: &mut BloomFilter<128>, n: &mut i32, addr: &IpAddr) {
    let h = hash_address(addr);
    if !ips.find(&h) {
        ips.set(&h);
        *n += 1;
    }
}

/// `pick_least_important_item` — score = `num_announcers/5 -
/// min_distance_exp`; the minimum is evicted.
fn least_important<V>(
    node_ids: &[NodeId],
    table: &BTreeMap<NodeId, V>,
    announcers: impl Fn(&V) -> i32,
) -> Option<NodeId> {
    table
        .iter()
        .min_by(|(lk, lv), (rk, rv)| {
            let l = announcers(lv) / 5 - min_distance_exp(lk, node_ids);
            let r = announcers(rv) / 5 - min_distance_exp(rk, node_ids);
            l.cmp(&r)
        })
        .map(|(k, _)| *k)
}

/// `get_mutable_item` reply.
#[derive(Debug)]
pub struct MutableItemRef<'a> {
    /// `seq`.
    pub seq: SequenceNumber,
    /// `v` — present when `force_fill` or newer.
    pub value: Option<&'a [u8]>,
    /// `k`.
    pub key: &'a PublicKey,
    /// `sig`.
    pub sig: &'a Signature64,
    /// `salt`.
    pub salt: &'a [u8],
}

/// `put_mutable_item` arguments.
pub struct PutMutable<'a> {
    /// `buf` — bencoded value.
    pub value: &'a [u8],
    /// `sig`.
    pub sig: &'a Signature64,
    /// `seq`.
    pub seq: SequenceNumber,
    /// `pk`.
    pub pk: &'a PublicKey,
    /// `salt`.
    pub salt: &'a [u8],
    /// announcer address (bloom accounting).
    pub addr: &'a IpAddr,
}

/// `dht_default_storage`.
#[derive(Clone)]
pub struct DhtStorage {
    /// `m_settings`.
    settings: DhtSettings,
    /// `m_counters`.
    counters: DhtStorageCounters,
    /// `m_node_ids` — our node ids (distance reference for eviction).
    pub node_ids: Vec<NodeId>,
    /// `m_map` — info_hash → peers.
    map: BTreeMap<Sha1Hash, TorrentEntry>,
    /// `m_immutable_table`.
    immutable: BTreeMap<NodeId, DhtImmutableItem>,
    /// `m_mutable_table`.
    mutable: BTreeMap<NodeId, DhtMutableItem>,
    /// `m_infohashes_sample`.
    infohashes_sample: Vec<Sha1Hash>,
    /// `created` stamp for the sample.
    sample_created: Option<Instant>,
}

impl DhtStorage {
    /// `dht_default_storage(settings)` — `node_ids` feeds
    /// eviction distance scoring.
    #[must_use]
    pub fn new(settings: DhtSettings, node_ids: Vec<NodeId>) -> Self {
        Self {
            settings,
            counters: DhtStorageCounters::default(),
            node_ids,
            map: BTreeMap::new(),
            immutable: BTreeMap::new(),
            mutable: BTreeMap::new(),
            infohashes_sample: Vec::new(),
            sample_created: None,
        }
    }

    /// `counters()`.
    #[must_use]
    pub fn counters(&self) -> DhtStorageCounters {
        self.counters
    }

    /// `get_peers` — returns `None` when the info-hash is unknown.
    /// `no_token` mirrors upstream's return value (`true` when the
    /// table is full or the requester isn't in a full peer set).
    pub fn get_peers(
        &self,
        info_hash: &Sha1Hash,
        noseed: bool,
        scrape: bool,
        requester: &IpAddr,
    ) -> GetPeersReply {
        let Some(v) = self.map.get(info_hash) else {
            return GetPeersReply {
                no_token: self.map.len() >= self.settings.max_torrents as usize,
                ..Default::default()
            };
        };
        let peersv = if requester.is_ipv4() {
            &v.peers4
        } else {
            &v.peers6
        };
        let mut reply = GetPeersReply {
            name: (!v.name.is_empty()).then(|| v.name.clone()),
            ..Default::default()
        };

        if scrape {
            let mut downloaders = BloomFilter::<256>::default();
            let mut seeds = BloomFilter::<256>::default();
            for p in peersv {
                let h = hash_address(&p.addr.ip());
                if p.seed {
                    seeds.set(&h);
                } else {
                    downloaders.set(&h);
                }
            }
            reply.bfpe = Some(downloaders.as_bytes().to_vec());
            reply.bfsd = Some(seeds.as_bytes().to_vec());
        } else {
            let mut to_pick = self.settings.max_peers_reply.max(0);
            // v6 peers are 4× bigger — divide the reply budget
            if !peersv.is_empty() && requester.is_ipv6() {
                to_pick /= 4;
            }
            let mut candidates = peersv.iter().filter(|e| !(noseed && e.seed)).count() as u32;
            to_pick = to_pick.min(candidates as i32);
            let mut out = Vec::new();
            for e in peersv {
                if to_pick <= 0 {
                    break;
                }
                if noseed && e.seed {
                    continue;
                }
                // pick with probability to_pick/candidates
                if random(candidates) > to_pick as u32 {
                    candidates = candidates.saturating_sub(1);
                    continue;
                }
                candidates = candidates.saturating_sub(1);
                write_endpoint(e.addr, &mut out);
                to_pick -= 1;
            }
            reply.peers = Some(out);
        }

        // full peer set → only in-set requesters get a write token
        if peersv.len() < self.settings.max_peers as usize {
            reply.no_token = false;
        } else {
            let in_set = peersv.iter().any(|p| p.addr.ip() == *requester);
            reply.no_token = !in_set;
        }
        reply
    }

    /// `announce_peer`.
    pub fn announce_peer(
        &mut self,
        info_hash: &Sha1Hash,
        endp: SocketAddr,
        name: &str,
        seed: bool,
    ) {
        let v = match self.map.get_mut(info_hash) {
            Some(v) => v,
            None => {
                if self.map.len() >= self.settings.max_torrents as usize {
                    return;
                }
                self.counters.torrents += 1;
                self.map.entry(*info_hash).or_default()
            }
        };
        if !name.is_empty() && v.name.is_empty() {
            // upstream `name.substr(0, 100)` — a *byte* limit; snap
            // to a char boundary so the Rust `String` stays valid.
            let end = (0..=name.len().min(100))
                .rev()
                .find(|&i| name.is_char_boundary(i))
                .unwrap_or(0);
            v.name = name[..end].to_string();
        }
        let peersv = if endp.is_ipv4() {
            &mut v.peers4
        } else {
            &mut v.peers6
        };
        let peer = PeerEntry {
            added: Instant::now(),
            addr: endp,
            seed,
        };
        // peers are kept sorted by endpoint (lower_bound upstream)
        match peersv
            .binary_search_by(|p| (p.addr.ip(), p.addr.port()).cmp(&(endp.ip(), endp.port())))
        {
            Ok(i) => peersv[i] = peer,
            Err(i) => {
                // one host can receive on every address of its /56 —
                // bound how many entries per info-hash a prefix claims
                if let IpAddr::V6(a6) = endp.ip() {
                    let prefix = a6.octets();
                    let same_pfx = |e: &PeerEntry| match e.addr {
                        SocketAddr::V6(p) => same_v6_prefix(&prefix, &p.ip().octets()),
                        SocketAddr::V4(_) => false,
                    };
                    let mut first = i;
                    while first > 0 && same_pfx(&peersv[first - 1]) {
                        first -= 1;
                    }
                    let mut last = i;
                    while last < peersv.len() && same_pfx(&peersv[last]) {
                        last += 1;
                    }
                    if last - first >= MAX_PEERS_PER_PREFIX {
                        // replace rather than drop — a peer in a
                        // contested prefix still gets in
                        let oldest = (first..last)
                            .min_by_key(|&k| peersv[k].added)
                            .unwrap_or(first);
                        peersv[oldest] = peer;
                        // the new entry belongs within the run —
                        // sorting it restores order
                        peersv[first..last].sort_by(|a, b| {
                            (a.addr.ip(), a.addr.port()).cmp(&(b.addr.ip(), b.addr.port()))
                        });
                        return;
                    }
                }
                if peersv.len() >= self.settings.max_peers as usize {
                    return;
                }
                peersv.insert(i, peer);
                self.counters.peers += 1;
            }
        }
    }

    /// `get_immutable_item` — returns the raw bencoded value.
    #[must_use]
    pub fn get_immutable_item(&self, target: &NodeId) -> Option<&[u8]> {
        self.immutable.get(target).map(|i| &i.value[..])
    }

    /// `put_immutable_item`.
    pub fn put_immutable_item(&mut self, target: &NodeId, buf: &[u8], addr: &IpAddr) {
        if let Some(i) = self.immutable.get_mut(target) {
            // `set_value` overwrites the payload in place
            i.value = buf.to_vec();
        } else {
            if self.immutable.len() >= self.settings.max_dht_items as usize {
                if let Some(k) =
                    least_important(&self.node_ids, &self.immutable, |i| i.num_announcers)
                {
                    self.immutable.remove(&k);
                    self.counters.immutable_data -= 1;
                }
            }
            self.immutable.insert(
                *target,
                DhtImmutableItem {
                    value: buf.to_vec(),
                    ips: BloomFilter::default(),
                    last_seen: Instant::now(),
                    num_announcers: 0,
                },
            );
            self.counters.immutable_data += 1;
        }
        if let Some(i) = self.immutable.get_mut(target) {
            i.last_seen = Instant::now();
            touch(&mut i.ips, &mut i.num_announcers, addr);
        }
    }

    /// `get_mutable_item_seq`.
    #[must_use]
    pub fn get_mutable_item_seq(&self, target: &NodeId) -> Option<SequenceNumber> {
        self.mutable.get(target).map(|i| i.seq)
    }

    /// `get_mutable_item` — returns `(seq, value?, k, sig, salt)`.
    /// `value` present when `force_fill` or `seq < stored seq`.
    #[must_use]
    pub fn get_mutable_item(
        &self,
        target: &NodeId,
        seq: SequenceNumber,
        force_fill: bool,
    ) -> Option<MutableItemRef<'_>> {
        let f = self.mutable.get(target)?;
        let value = (force_fill || (0 <= seq && seq < f.seq)).then_some(&f.value[..]);
        Some(MutableItemRef {
            seq: f.seq,
            value,
            key: &f.key,
            sig: &f.sig,
            salt: &f.salt,
        })
    }

    /// `put_mutable_item` — signature/seq checks are the caller's
    /// (upstream `node` verifies before calling); here we keep the
    /// newest seq and record metadata.
    pub fn put_mutable_item(&mut self, target: &NodeId, put: PutMutable<'_>) {
        if !self.mutable.contains_key(target) {
            if self.mutable.len() >= self.settings.max_dht_items as usize {
                if let Some(k) =
                    least_important(&self.node_ids, &self.mutable, |i| i.num_announcers)
                {
                    self.mutable.remove(&k);
                    self.counters.mutable_data -= 1;
                }
            }
            self.mutable.insert(
                *target,
                DhtMutableItem {
                    value: put.value.to_vec(),
                    ips: BloomFilter::default(),
                    last_seen: Instant::now(),
                    num_announcers: 0,
                    sig: *put.sig,
                    seq: put.seq,
                    key: *put.pk,
                    salt: put.salt.to_vec(),
                },
            );
            self.counters.mutable_data += 1;
        } else if let Some(item) = self.mutable.get_mut(target) {
            if item.seq < put.seq {
                item.value = put.value.to_vec();
                item.seq = put.seq;
                item.sig = *put.sig;
            }
        }
        if let Some(i) = self.mutable.get_mut(target) {
            i.last_seen = Instant::now();
            touch(&mut i.ips, &mut i.num_announcers, put.addr);
        }
    }

    /// `get_infohashes_sample` (BEP-51) — refreshes the sample set
    /// when stale, returns `(interval, num_torrents, samples)`.
    pub fn get_infohashes_sample(&mut self) -> (i32, usize, &[Sha1Hash]) {
        let interval = self
            .settings
            .sample_infohashes_interval
            .clamp(0, SAMPLE_INFOHASHES_INTERVAL_MAX);
        let num = self.map.len();
        self.refresh_infohashes_sample();
        (interval, num, &self.infohashes_sample)
    }

    /// `refresh_infohashes_sample`.
    fn refresh_infohashes_sample(&mut self) {
        let now = Instant::now();
        let interval = self
            .settings
            .sample_infohashes_interval
            .clamp(0, SAMPLE_INFOHASHES_INTERVAL_MAX);
        let max_count = self
            .settings
            .max_infohashes_sample_count
            .clamp(0, INFOHASHES_SAMPLE_COUNT_MAX) as usize;
        let count = max_count.min(self.map.len());

        if interval > 0
            && self
                .sample_created
                .is_some_and(|c| c + Duration::from_secs(interval as u64) > now)
            && self.infohashes_sample.len() >= max_count
        {
            return;
        }
        self.infohashes_sample.clear();
        self.infohashes_sample.reserve(count);
        let mut to_pick = count as u32;
        let mut candidates = self.map.len() as u32;
        for k in self.map.keys() {
            if to_pick == 0 {
                break;
            }
            if random(candidates) > to_pick {
                candidates = candidates.saturating_sub(1);
                continue;
            }
            candidates = candidates.saturating_sub(1);
            self.infohashes_sample.push(*k);
            to_pick -= 1;
        }
        self.sample_created = Some(now);
    }

    /// `purge_peers` — drop peers older than 1.5 announce
    /// intervals. Returns the number purged.
    fn purge_peers(peers: &mut Vec<PeerEntry>) -> usize {
        let now = Instant::now();
        let before = peers.len();
        peers.retain(|e| e.added + ANNOUNCE_INTERVAL * 3 / 2 >= now);
        if !peers.is_empty() && peers.capacity() / peers.len().max(1) >= 4 {
            peers.shrink_to_fit();
        }
        before - peers.len()
    }

    /// `tick` — expire peers, then items past `item_lifetime`
    /// (0 = disabled; floor of 120 min upstream).
    pub fn tick(&mut self) {
        let mut empty: Vec<Sha1Hash> = Vec::new();
        let mut purged = 0usize;
        for (k, t) in self.map.iter_mut() {
            purged += Self::purge_peers(&mut t.peers4);
            purged += Self::purge_peers(&mut t.peers6);
            if t.peers4.is_empty() && t.peers6.is_empty() {
                empty.push(*k);
            }
        }
        self.counters.peers -= purged as i32;
        for k in empty {
            self.map.remove(&k);
            self.counters.torrents -= 1;
        }

        if self.settings.item_lifetime == 0 {
            return;
        }
        let now = Instant::now();
        let lifetime = Duration::from_secs(self.settings.item_lifetime.max(120 * 60) as u64);
        self.immutable.retain(|_, i| {
            let keep = i.last_seen + lifetime > now;
            if !keep {
                self.counters.immutable_data -= 1;
            }
            keep
        });
        self.mutable.retain(|_, i| {
            let keep = i.last_seen + lifetime > now;
            if !keep {
                self.counters.mutable_data -= 1;
            }
            keep
        });
    }

    /// read access to the torrent map (tests/metrics).
    #[must_use]
    pub fn num_torrents(&self) -> usize {
        self.map.len()
    }
}
