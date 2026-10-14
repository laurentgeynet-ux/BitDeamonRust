//! Port of `ip_voter.cpp` — external-IP voting: peers/trackers/DHT
//! report what they believe our external IP is; we adopt the majority
//! once the vote count thresholds are met.

use std::net::IpAddr;
use std::time::{Duration, Instant};

use bitdaemon_common::bloom_filter::BloomFilter;
use bitdaemon_common::random::random;

use crate::ip_helpers::{hash_address, is_local};

bitdaemon_common::bitfield_flag! {
    /// `ip_source_t` (`session_interface.hpp`).
    IpSourceFlags, u8
}

impl IpSourceFlags {
    /// `source_dht` — upstream `1_bit`.
    pub const SOURCE_DHT: Self = Self::bit(1);
    /// `source_peer` — upstream `2_bit`.
    pub const SOURCE_PEER: Self = Self::bit(2);
    /// `source_tracker` — upstream `3_bit`.
    pub const SOURCE_TRACKER: Self = Self::bit(3);
    /// `source_router` — upstream `4_bit`.
    pub const SOURCE_ROUTER: Self = Self::bit(4);
}

/// `external_ip_t` — per-candidate state.
struct ExternalIpEntry {
    addr: IpAddr,
    /// who voted for this (bloom filter of voter keys)
    voters: BloomFilter<16>,
    /// which sources told us
    sources: IpSourceFlags,
    num_votes: u16,
}

/// Upstream `external_ip_t::operator<` sorts *descending* — more votes
/// first, ties broken toward the higher `sources` bitmask (more
/// reliable reporters win).
fn upstream_order(a: &ExternalIpEntry, b: &ExternalIpEntry) -> std::cmp::Ordering {
    b.num_votes
        .cmp(&a.num_votes)
        .then(b.sources.bits().cmp(&a.sources.bits()))
}

impl ExternalIpEntry {
    /// `add_vote` — false when this voter already voted.
    fn add_vote(&mut self, k: &bitdaemon_common::hash::Sha1Hash, t: IpSourceFlags) -> bool {
        self.sources |= t;
        if self.voters.find(k) {
            return false;
        }
        self.voters.set(k);
        self.num_votes += 1;
        true
    }
}

/// `ip_voter` — majority-vote external IP.
pub struct IpVoter {
    m_external_addresses: Vec<ExternalIpEntry>,
    m_external_address_voters: BloomFilter<32>,
    m_external_address: Option<IpAddr>,
    m_total_votes: i32,
    m_last_rotate: Instant,
    m_valid_external: bool,
}

impl Default for IpVoter {
    fn default() -> Self {
        Self::new()
    }
}

impl IpVoter {
    /// `ip_voter()`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            m_external_addresses: Vec::new(),
            m_external_address_voters: BloomFilter::new(),
            m_external_address: None,
            m_total_votes: 0,
            m_last_rotate: Instant::now(),
            m_valid_external: false,
        }
    }

    /// `external_address()`.
    #[must_use]
    pub fn external_address(&self) -> Option<IpAddr> {
        self.m_external_address
    }

    /// `cast_vote(ip, source_type, source)` — true when the external
    /// IP changed.
    pub fn cast_vote(&mut self, ip: IpAddr, source_type: IpSourceFlags, source: &IpAddr) -> bool {
        if ip.is_unspecified() || is_local(&ip) || ip.is_loopback() {
            return false;
        }
        // don't trust a source reporting a different address family
        if ip.is_ipv4() != source.is_ipv4() {
            return false;
        }

        let k = hash_address(source);

        let i = self.m_external_addresses.iter().position(|e| e.addr == ip);

        let idx = match i {
            Some(i) => i,
            None => {
                // each voter only gets to add one new IP
                if self.m_external_address_voters.find(&k) {
                    return self.maybe_rotate();
                }
                if self.m_external_addresses.len() > 40 {
                    if random(1) != 0 {
                        return self.maybe_rotate();
                    }
                    // upstream stable-sorts with the descending
                    // comparator and erases the last element — the
                    // entry with the fewest votes (newest on ties)
                    self.m_external_addresses.sort_by(upstream_order);
                    self.m_external_addresses.pop();
                }
                self.m_external_addresses.push(ExternalIpEntry {
                    addr: ip,
                    voters: BloomFilter::new(),
                    sources: IpSourceFlags::default(),
                    num_votes: 0,
                });
                self.m_external_address_voters.set(&k);
                self.m_external_addresses.len() - 1
            }
        };

        if !self.m_external_addresses[idx].add_vote(&k, source_type) {
            return self.maybe_rotate();
        }
        self.m_total_votes += 1;

        if self.m_valid_external {
            return self.maybe_rotate();
        }

        // upstream `min_element` under the descending comparator —
        // the leading candidate (most votes, then most reliable
        // sources; first on full ties)
        let min_votes_addr = self
            .m_external_addresses
            .iter()
            .min_by(|a, b| upstream_order(a, b))
            .map_or(ip, |e| e.addr);
        if Some(min_votes_addr) == self.m_external_address {
            return self.maybe_rotate();
        }
        if self.m_external_address.is_some() {
            return self.m_total_votes >= 25 && self.maybe_rotate();
        }
        self.m_external_address = Some(min_votes_addr);
        true
    }

    /// `maybe_rotate` — decide whether to lock in the top vote.
    fn maybe_rotate(&mut self) -> bool {
        let now = Instant::now();
        if self.m_total_votes < 50
            && (now.duration_since(self.m_last_rotate) < Duration::from_secs(300)
                || self.m_total_votes == 0)
            && self.m_valid_external
        {
            return false;
        }
        if self.m_external_addresses.is_empty() {
            return false;
        }
        if self.m_external_addresses.len() == 1 {
            if self.m_external_addresses[0].num_votes < 2 {
                return false;
            }
        } else {
            // upstream `partial_sort(begin, begin + 2, end)` — the two
            // leading candidates under the descending comparator
            self.m_external_addresses
                .select_nth_unstable_by(1, upstream_order);
            if self.m_external_addresses[0].num_votes * 2 / 3
                <= self.m_external_addresses[1].num_votes
            {
                return false;
            }
        }
        let top = self.m_external_addresses[0].addr;
        let ret = self.m_external_address != Some(top);
        self.m_external_address = Some(top);
        self.m_external_address_voters.clear();
        self.m_total_votes = 0;
        self.m_external_addresses.clear();
        self.m_last_rotate = now;
        self.m_valid_external = true;
        ret
    }
}

/// `external_ip` — one address per (local|global) × (v4|v6) cell, as
/// observed by a peer at `ip`. `[0][n]` = global, `[1][n]` = local,
/// `[n][0]` = IPv4, `[n][1]` = IPv6.
#[derive(Clone, Copy, Debug)]
pub struct ExternalIp {
    addrs: [[IpAddr; 2]; 2],
}

impl Default for ExternalIp {
    fn default() -> Self {
        Self {
            addrs: [
                [
                    IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                    IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
                ],
                [
                    IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                    IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
                ],
            ],
        }
    }
}

impl ExternalIp {
    /// `external_ip(local4, global4, local6, global6)` — a v4
    /// `unspecified` in the v6 slots maps to v6 `unspecified`
    /// (`ensure_v6`).
    #[must_use]
    pub fn new(local4: IpAddr, global4: IpAddr, local6: IpAddr, global6: IpAddr) -> Self {
        Self {
            addrs: [
                [global4, crate::ip_helpers::ensure_v6(global6)],
                [local4, crate::ip_helpers::ensure_v6(local6)],
            ],
        }
    }

    /// `external_address(ip)` — the external IP as observed from `ip`.
    /// A v6 query answered by an unspecified v4 maps to unspecified v6.
    #[must_use]
    pub fn external_address(&self, ip: &IpAddr) -> IpAddr {
        let ext =
            self.addrs[usize::from(crate::ip_helpers::is_local(ip))][usize::from(ip.is_ipv6())];
        if ip.is_ipv6() && ext == IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED) {
            return IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED);
        }
        ext
    }
}
