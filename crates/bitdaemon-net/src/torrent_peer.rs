//! Port of `aux_/torrent_peer.hpp`/`torrent_peer.cpp` — the
//! torrent-side peer record: address variant (upstream uses a
//! subclass hierarchy `ipv4_peer`/`ipv6_peer`/`i2p_peer`), the packed
//! bitfield of connection state flags, download/upload accounting in
//! kiB, and `peer_priority` (BEP 40 rank).

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use bitdaemon_common::PeerSourceFlags;
use bitdaemon_common::crc32c::crc32c;

/// The address part of a `torrent_peer` — upstream models this as the
/// `ipv4_peer` / `ipv6_peer` / `i2p_peer` subclasses.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PeerAddr {
    /// `ipv4_peer::addr`.
    V4(Ipv4Addr),
    /// `ipv6_peer::addr` — 16 raw bytes.
    V6(Ipv6Addr),
    /// `i2p_peer::destination` — the base64 destination string.
    I2p(Vec<u8>),
}

impl PeerAddr {
    /// `torrent_peer::address()` — `None` for i2p (upstream returns
    /// the default-constructed `address{}`).
    #[must_use]
    pub fn address(&self) -> Option<IpAddr> {
        match self {
            Self::V4(a) => Some(IpAddr::V4(*a)),
            Self::V6(a) => Some(IpAddr::V6(*a)),
            Self::I2p(_) => None,
        }
    }

    /// `is_v6_addr` bit.
    #[must_use]
    pub fn is_v6(&self) -> bool {
        matches!(self, Self::V6(_))
    }

    /// `is_i2p_addr` bit.
    #[must_use]
    pub fn is_i2p(&self) -> bool {
        matches!(self, Self::I2p(_))
    }
}

/// `protocol_version` (`V1`/`V2` upstream enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolVersion {
    /// `protocol_version::V1`.
    V1,
    /// `protocol_version::V2`.
    V2,
}

/// `peer_connection_interface` statistics access — the trait the live
/// connection implements so `total_download`/`total_upload` read from
/// it while connected (upstream dereferences `connection`).
pub trait PeerStats: Send + Sync {
    /// `statistics().total_payload_download()`.
    fn total_payload_download(&self) -> i64;
    /// `statistics().total_payload_upload()`.
    fn total_payload_upload(&self) -> i64;
}

/// Null `PeerStats` used by `peer_list` to mark "has a connection"
/// without owning the `peer_connection` (upstream stores a pointer).
#[derive(Debug)]
pub struct NullStats;

impl PeerStats for NullStats {
    fn total_payload_download(&self) -> i64 {
        0
    }
    fn total_payload_upload(&self) -> i64 {
        0
    }
}

/// `aux::torrent_peer`. Bitfield flags are stored as named `bool`/
/// small-int fields — identical semantics to the packed bitfield
/// (upstream widths documented per field).
pub struct TorrentPeer {
    /// The address variant (`ipv4_peer`/`ipv6_peer`/`i2p_peer`).
    pub addr: PeerAddr,

    /// `prev_amount_upload` — accumulated upload in **kiB** (shift
    /// left 10 for bytes), only counted once the connection closes.
    pub prev_amount_upload: u32,
    /// `prev_amount_download` — same for download.
    pub prev_amount_download: u32,

    /// `connection` — live connection stats while connected.
    pub connection: Option<Arc<dyn PeerStats>>,

    /// which live connection `connection` refers to — the engine's
    /// conn id (`peer` key in `torrent::peers`), `0` while an
    /// outbound dial is still pending (the conn object doesn't
    /// exist yet upstream either — `m_peer_info` is bound at the
    /// `peer_connection` ctor). Upstream dereferences
    /// `c.peer_info_struct()`; we re-resolve by this key so a port
    /// rewrite via `update_peer_port` can't break the link.
    pub conn_key: u64,

    /// `peer_rank` — lazily computed BEP-40 priority (0 = not set).
    /// `Cell`: upstream's `rank()` is `const` but memoizes into the
    /// `mutable` bitfield member, and `compare_peer` only holds `&`.
    pub peer_rank: std::cell::Cell<u32>,

    /// `last_optimistically_unchoked` — seconds since session start,
    /// wraps at ~18.2 h.
    pub last_optimistically_unchoked: u16,
    /// `last_connected` — same epoch semantics.
    pub last_connected: u16,

    /// `port`.
    pub port: u16,

    /// `hashfails` — pieces that failed hash checks with this peer.
    pub hashfails: u8,

    /// `failcount:5` — [0, 31] failed connection attempts.
    pub failcount: u8,
    /// `connectable:1`.
    pub connectable: bool,
    /// `optimistically_unchoked:1`.
    pub optimistically_unchoked: bool,
    /// `seed:1`.
    pub seed: bool,
    /// `upload_only:1`.
    pub upload_only: bool,
    /// `maybe_upload_only:1`.
    pub maybe_upload_only: bool,
    /// `fast_reconnects:4` — [0, 15].
    pub fast_reconnects: u8,
    /// `trust_points:4` — [-7, 8].
    pub trust_points: i8,
    /// `source:6` — `peer_source_flags_t`.
    pub source: PeerSourceFlags,
    /// `pe_support:1` — supports MSE/PE encryption.
    pub pe_support: bool,
    /// `on_parole:1` — pieces from this peer get a dedicated hash
    /// check before counting.
    pub on_parole: bool,
    /// `banned:1`.
    pub banned: bool,
    /// `supports_utp:1` — assumed true upstream.
    pub supports_utp: bool,
    /// `confirmed_supports_utp:1`.
    pub confirmed_supports_utp: bool,
    /// `supports_holepunch:1`.
    pub supports_holepunch: bool,
    /// pending holepunch dial — upstream flags the synchronously
    /// created conn via `set_holepunch_mode` inside `on_holepunch`'s
    /// connect arm; our dial is async, so the entry carries the flag
    /// until the attach copies it onto `conn.holepunch_mode`.
    pub holepunch: bool,
    /// `web_seed:1`.
    pub web_seed: bool,
    /// `protocol_v2:1`.
    pub protocol_v2: bool,
}

impl TorrentPeer {
    /// `torrent_peer(port, connectable, src)` + subclass ctor
    /// (`ipv4_peer(ep, connectable, src)` etc.).
    #[must_use]
    pub fn new(addr: PeerAddr, port: u16, connectable: bool, src: PeerSourceFlags) -> Self {
        Self {
            addr,
            prev_amount_upload: 0,
            prev_amount_download: 0,
            connection: None,
            conn_key: 0,
            peer_rank: std::cell::Cell::new(0),
            last_optimistically_unchoked: 0,
            last_connected: 0,
            port,
            hashfails: 0,
            failcount: 0,
            connectable,
            optimistically_unchoked: false,
            seed: false,
            upload_only: false,
            maybe_upload_only: false,
            fast_reconnects: 0,
            trust_points: 0,
            source: src,
            // assume no support in order to prefer opening
            // non-encrypted connections; retry with encryption on
            // failure (upstream comment)
            pe_support: false,
            on_parole: false,
            banned: false,
            // assume peers support utp
            supports_utp: true,
            confirmed_supports_utp: false,
            supports_holepunch: false,
            holepunch: false,
            web_seed: false,
            protocol_v2: false,
        }
    }

    /// `ipv4_peer(ep, connectable, src)`. A misrouted v6 endpoint is
    /// debug-asserted and stored as the v6 variant rather than
    /// panicking — the `PeerAddr` family is determined by the address.
    #[must_use]
    pub fn new_v4(ep: SocketAddr, connectable: bool, src: PeerSourceFlags) -> Self {
        debug_assert!(ep.is_ipv4(), "new_v4 requires an IPv4 endpoint");
        match ep {
            SocketAddr::V4(a) => Self::new(PeerAddr::V4(*a.ip()), a.port(), connectable, src),
            SocketAddr::V6(a) => Self::new(PeerAddr::V6(*a.ip()), a.port(), connectable, src),
        }
    }

    /// `ipv6_peer(ep, connectable, src)` — see [`Self::new_v4`].
    #[must_use]
    pub fn new_v6(ep: SocketAddr, connectable: bool, src: PeerSourceFlags) -> Self {
        debug_assert!(ep.is_ipv6(), "new_v6 requires an IPv6 endpoint");
        match ep {
            SocketAddr::V6(a) => Self::new(PeerAddr::V6(*a.ip()), a.port(), connectable, src),
            SocketAddr::V4(a) => Self::new(PeerAddr::V4(*a.ip()), a.port(), connectable, src),
        }
    }

    /// `i2p_peer(dest, connectable, src)` — `port` is 0.
    #[must_use]
    pub fn new_i2p(dest: Vec<u8>, connectable: bool, src: PeerSourceFlags) -> Self {
        Self::new(PeerAddr::I2p(dest), 0, connectable, src)
    }

    /// `ip()` — the endpoint (`tcp::endpoint(address(), port)`); for
    /// i2p upstream returns a default endpoint, here `None`.
    #[must_use]
    pub fn ip(&self) -> Option<SocketAddr> {
        self.addr.address().map(|a| SocketAddr::new(a, self.port))
    }

    /// `protocol()`.
    #[must_use]
    pub fn protocol(&self) -> ProtocolVersion {
        if self.protocol_v2 {
            ProtocolVersion::V2
        } else {
            ProtocolVersion::V1
        }
    }

    /// `total_download()` — live stats while connected, else the
    /// accumulated kiB.
    #[must_use]
    pub fn total_download(&self) -> i64 {
        match &self.connection {
            Some(c) => {
                debug_assert_eq!(self.prev_amount_download, 0);
                c.total_payload_download()
            }
            None => i64::from(self.prev_amount_download) * 1024,
        }
    }

    /// `total_upload()`.
    #[must_use]
    pub fn total_upload(&self) -> i64 {
        match &self.connection {
            Some(c) => {
                debug_assert_eq!(self.prev_amount_upload, 0);
                c.total_payload_upload()
            }
            None => i64::from(self.prev_amount_upload) * 1024,
        }
    }

    /// `rank(external, external_port)` — lazily computes
    /// `peer_priority` between the external address of the peer's
    /// family (`external_ip::external_address` — unspecified when
    /// unset) and the peer's endpoint. Upstream does not special-case
    /// i2p (`address()` → v4-unspecified) or missing externals — the
    /// CRC runs on whatever pair results.
    #[must_use]
    pub fn rank(&self, external: &crate::ip_voter::ExternalIp, external_port: u16) -> u32 {
        if self.peer_rank.get() == 0 {
            let local = self
                .addr
                .address()
                .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
            self.peer_rank.set(peer_priority(
                SocketAddr::new(external.external_address(&local), external_port),
                SocketAddr::new(local, self.port),
            ));
        }
        self.peer_rank.get()
    }
}

/// `apply_mask` — byte-wise AND.
fn apply_mask(b: &mut [u8], mask: &[u8]) {
    for (b, m) in b.iter_mut().zip(mask.iter()) {
        *b &= m;
    }
}

/// `peer_priority(e1, e2)` — BEP 40: CRC32C over the ordered,
/// prefix-masked endpoint pair. Both endpoints must share an address
/// family.
///
/// 1. identical IPs → hash the two ports (16-bit, big-endian, lowest
///    first);
/// 2. same /24 → hash both full IPs, ordered;
/// 3. same /16 → mask by `0xffffff55`, hash ordered;
/// 4. different /16 → mask by `0xffff5555`, hash ordered.
///
/// IPv6: compare /48, /56, /64 …; mask `0x55` on every byte after the
/// first differing byte (but never before byte 5); the lower 48 bits
/// are always unmasked.
#[must_use]
pub fn peer_priority(e1: SocketAddr, e2: SocketAddr) -> u32 {
    debug_assert_eq!(e1.is_ipv4(), e2.is_ipv4());
    if e1.is_ipv4() != e2.is_ipv4() {
        // upstream TORRENT_ASSERTs on the caller; a mixed pair can
        // only come from a bug — return 0 rather than panic.
        return 0;
    }
    let (e1, e2) = if e1 > e2 { (e2, e1) } else { (e1, e2) };

    if e1.ip() == e2.ip() {
        let mut buf = [0u8; 4];
        buf[0..2].copy_from_slice(&e1.port().to_be_bytes());
        buf[2..4].copy_from_slice(&e2.port().to_be_bytes());
        // upstream memcpys into a uint32 then feeds crc32c_32 (which
        // re-serializes LE) — net effect: crc32c over the BE ports
        return crc32c(&buf);
    }

    match (e1.ip(), e2.ip()) {
        (IpAddr::V6(a1), IpAddr::V6(a2)) => {
            let mut b1 = a1.octets();
            let mut b2 = a2.octets();
            let mut offset = usize::MAX;
            for i in 0..16 {
                if offset == usize::MAX && b1[i] != b2[i] {
                    offset = (i + 1).max(5);
                } else if i > offset {
                    b1[i] &= 0x55;
                    b2[i] &= 0x55;
                }
            }
            let mut buf = [0u8; 32];
            buf[..16].copy_from_slice(&b1);
            buf[16..].copy_from_slice(&b2);
            crc32c(&buf)
        }
        (IpAddr::V4(a1), IpAddr::V4(a2)) => {
            const V4MASK: [[u8; 4]; 3] = [
                [0xff, 0xff, 0x55, 0x55],
                [0xff, 0xff, 0xff, 0x55],
                [0xff, 0xff, 0xff, 0xff],
            ];
            let mut b1 = a1.octets();
            let mut b2 = a2.octets();
            let mask = if b1[..2] != b2[..2] {
                0
            } else if b1[..3] != b2[..3] {
                1
            } else {
                2
            };
            apply_mask(&mut b1, &V4MASK[mask]);
            apply_mask(&mut b2, &V4MASK[mask]);
            let mut buf = [0u8; 8];
            buf[..4].copy_from_slice(&b1);
            buf[4..].copy_from_slice(&b2);
            crc32c(&buf)
        }
        _ => 0,
    }
}
