//! Port of `ip_filter.hpp`/`ip_filter.cpp` — `ip_filter` (IPv4 + IPv6
//! range → access flags), `port_filter`, and `ip_voter` comes later.
//!
//! `filter_impl<Addr>` maps to `FilterImpl<A>` over `BTreeMap<A, u32>`
//! keyed on range start — same implicit-end semantics as upstream's
//! `std::set<range>` (a range's end is the next entry's start).

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::ops::Bound;

/// `ip_filter::blocked`.
pub const BLOCKED: u32 = 1;
/// `ip_filter` allow flag.
pub const ALLOWED: u32 = 0;

/// `ip_range` — exported rule triple.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpRange<A> {
    /// `first`.
    pub first: A,
    /// `last`.
    pub last: A,
    /// `flags`.
    pub flags: u32,
}

/// `aux::filter_impl<Addr>` primitives: `zero`, `plus_one`,
/// `minus_one`, `max_addr` — big-endian byte-order addressing.
pub trait FilterAddr: Ord + Copy {
    /// `zero<Addr>()`.
    fn zero() -> Self;
    /// `plus_one` — carry-propagating increment.
    fn plus_one(self) -> Self;
    /// `minus_one`.
    fn minus_one(self) -> Self;
    /// `max_addr`.
    fn max_addr() -> Self;
}

impl FilterAddr for u16 {
    fn zero() -> Self {
        0
    }
    fn plus_one(self) -> Self {
        self.wrapping_add(1)
    }
    fn minus_one(self) -> Self {
        self.wrapping_sub(1)
    }
    fn max_addr() -> Self {
        u16::MAX
    }
}

// byte-array big-endian carry, verbatim
macro_rules! impl_filter_addr_bytes {
    ($t:ty, $n:expr) => {
        impl FilterAddr for $t {
            fn zero() -> Self {
                [0; $n]
            }
            fn plus_one(self) -> Self {
                let mut tmp = self;
                for i in (0..$n).rev() {
                    if tmp[i] < u8::MAX {
                        tmp[i] += 1;
                        break;
                    }
                    tmp[i] = 0;
                }
                tmp
            }
            fn minus_one(self) -> Self {
                let mut tmp = self;
                for i in (0..$n).rev() {
                    if tmp[i] > 0 {
                        tmp[i] -= 1;
                        break;
                    }
                    tmp[i] = u8::MAX;
                }
                tmp
            }
            fn max_addr() -> Self {
                [u8::MAX; $n]
            }
        }
    };
}

impl_filter_addr_bytes!([u8; 4], 4);
impl_filter_addr_bytes!([u8; 16], 16);

/// `aux::filter_impl<Addr>` — sorted non-overlapping ranges; each
/// entry's end is the next entry's start.
#[derive(Debug, Clone)]
pub struct FilterImpl<A: FilterAddr> {
    /// `m_access_list` — start → access flags.
    access_list: BTreeMap<A, u32>,
}

impl<A: FilterAddr> Default for FilterImpl<A> {
    fn default() -> Self {
        let mut access_list = BTreeMap::new();
        // the entire range is allowed by default
        access_list.insert(A::zero(), 0);
        Self { access_list }
    }
}

impl<A: FilterAddr> FilterImpl<A> {
    /// `filter_impl()` — whole range allowed.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `empty()` — no rules (a sole `{0 → 0}` entry counts as empty).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.access_list.is_empty()
            || (self.access_list.len() == 1
                && self
                    .access_list
                    .first_key_value()
                    .is_some_and(|(&k, &v)| k == A::zero() && v == 0))
    }

    /// `add_rule(first, last, flags)` — last write wins on overlap.
    /// Transcription of upstream's `std::set` iterator surgery onto
    /// `BTreeMap`: `i` = entry containing `first`, `j` = first entry
    /// starting after `last`.
    pub fn add_rule(&mut self, first: A, last: A, flags: u32) {
        debug_assert!(!self.access_list.is_empty());
        debug_assert!(first <= last);

        // i0: entry containing `first` (always exists — zero entry).
        let i0 = self
            .access_list
            .range(..=first)
            .next_back()
            .map(|(&k, _)| k)
            .unwrap_or_else(A::zero);
        // j: first entry strictly after `last`.
        let j = self
            .access_list
            .range((Bound::Excluded(last), Bound::Unbounded))
            .next()
            .map(|(&k, _)| k);

        let mut first_access = self.access_list[&i0];
        let last_access = self.access(last);

        let i: A = if i0 != first && first_access != flags {
            self.access_list.insert(first, flags);
            first
        } else {
            // merge left when the previous entry already carries `flags`
            match self.access_list.range(..i0).next_back() {
                Some((&pk, &pv)) if pv == flags => {
                    first_access = pv;
                    pk
                }
                _ => i0,
            }
        };

        // erase (i, j)
        let doomed: Vec<A> = match j {
            Some(jk) => self
                .access_list
                .range((Bound::Excluded(i), Bound::Excluded(jk)))
                .map(|(&k, _)| k)
                .collect(),
            None => self
                .access_list
                .range((Bound::Excluded(i), Bound::Unbounded))
                .map(|(&k, _)| k)
                .collect(),
        };
        for k in doomed {
            self.access_list.remove(&k);
        }

        if i == first {
            if let Some(v) = self.access_list.get_mut(&i) {
                *v = flags;
            }
        } else if first_access != flags {
            self.access_list.insert(first, flags);
        }

        // re-open the right side: restore `last_access` after `last`
        // unless j already continues it or `last` is the range end
        let need_tail = match j {
            Some(jk) => jk.minus_one() != last,
            None => last != A::max_addr(),
        };
        // upstream reassigns `j` to the inserted tail element, so the
        // final merge-erase below can only drop the original `j` when
        // no tail was inserted
        let mut j_key = j;
        if need_tail && last_access != flags {
            let tail = last.plus_one();
            self.access_list.insert(tail, last_access);
            j_key = Some(tail);
        }

        // j got absorbed into the new range when its flags match
        if let Some(jk) = j_key {
            if self.access_list.get(&jk) == Some(&flags) {
                self.access_list.remove(&jk);
            }
        }
        debug_assert!(!self.access_list.is_empty());
    }

    /// `access(addr)` — O(log n) lookup.
    #[must_use]
    pub fn access(&self, addr: A) -> u32 {
        debug_assert!(!self.access_list.is_empty());
        self.access_list
            .range(..=addr)
            .next_back()
            .map(|(_, &v)| v)
            .unwrap_or(0)
    }

    /// `export_filter` — minimal range list, sorted.
    #[must_use]
    pub fn export_filter(&self) -> Vec<IpRange<A>> {
        let mut ret = Vec::with_capacity(self.access_list.len());
        let mut iter = self.access_list.iter().peekable();
        while let Some((&start, &flags)) = iter.next() {
            let last = match iter.peek() {
                Some(&(&next, _)) => next.minus_one(),
                None => A::max_addr(),
            };
            ret.push(IpRange {
                first: start,
                last,
                flags,
            });
        }
        ret
    }
}

/// `ip_filter` — IPv4 + IPv6 filter.
#[derive(Debug, Default, Clone)]
pub struct IpFilter {
    f4: FilterImpl<[u8; 4]>,
    f6: FilterImpl<[u8; 16]>,
}

impl IpFilter {
    /// `ip_filter()`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `empty`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.f4.is_empty() && self.f6.is_empty()
    }

    /// `add_rule` — `first`/`last` must be the same family (upstream
    /// asserts); unknown families are a no-op.
    pub fn add_rule(&mut self, first: IpAddr, last: IpAddr, flags: u32) {
        match (first, last) {
            (IpAddr::V4(f), IpAddr::V4(l)) => {
                self.f4.add_rule(f.octets(), l.octets(), flags);
            }
            (IpAddr::V6(f), IpAddr::V6(l)) => {
                self.f6.add_rule(f.octets(), l.octets(), flags);
            }
            _ => debug_assert!(false, "mismatched address families"),
        }
    }

    /// `access`.
    #[must_use]
    pub fn access(&self, addr: IpAddr) -> u32 {
        match addr {
            IpAddr::V4(a) => self.f4.access(a.octets()),
            IpAddr::V6(a) => self.f6.access(a.octets()),
        }
    }

    /// `export_filter` — `(v4 ranges, v6 ranges)`.
    #[must_use]
    pub fn export_filter(&self) -> (Vec<IpRange<Ipv4Addr>>, Vec<IpRange<Ipv6Addr>>) {
        let conv = |r: &IpRange<[u8; 4]>| IpRange {
            first: Ipv4Addr::from(r.first),
            last: Ipv4Addr::from(r.last),
            flags: r.flags,
        };
        let conv6 = |r: &IpRange<[u8; 16]>| IpRange {
            first: Ipv6Addr::from(r.first),
            last: Ipv6Addr::from(r.last),
            flags: r.flags,
        };
        (
            self.f4.export_filter().iter().map(conv).collect(),
            self.f6.export_filter().iter().map(conv6).collect(),
        )
    }
}

/// `port_filter`.
#[derive(Debug, Default, Clone)]
pub struct PortFilter {
    f: FilterImpl<u16>,
}

impl PortFilter {
    /// `port_filter()`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `add_rule(first, last, flags)`.
    pub fn add_rule(&mut self, first: u16, last: u16, flags: u32) {
        self.f.add_rule(first, last, flags);
    }

    /// `access(port)`.
    #[must_use]
    pub fn access(&self, port: u16) -> u32 {
        self.f.access(port)
    }

    /// `export_filter`.
    #[must_use]
    pub fn export_filter(&self) -> Vec<IpRange<u16>> {
        self.f.export_filter()
    }
}
