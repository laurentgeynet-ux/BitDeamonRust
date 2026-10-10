//! Typed bitfield flags — port of `flags::bitfield_flag<Underlying, Tag>`,
//! `torrent_flags.hpp`, `pex_flags.hpp`, `kademlia/announce_flags.hpp`.
//!
//! Upstream wraps an unsigned integer in a strongly-typed struct with
//! `|`/`&`/`^`/`~`, `operator bool`, `all()` and a `N_bit` literal
//! constructor. The [`bitfield_flag!`] macro below reproduces that surface
//! for a concrete flag type; constants keep upstream's bit positions.

/// Generates a `bitfield_flag` newtype: `bitfield_flag!(Name, u64)` then
/// `Name::bit(n)` / associated consts for each upstream flag.
#[macro_export]
macro_rules! bitfield_flag {
    ($(#[$meta:meta])* $name:ident, $ty:ty) => {
        $(#[$meta])*
        ///
        /// Port of `flags::bitfield_flag`.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
        pub struct $name(pub $ty);

        impl $name {
            /// `bitfield_flag()` — empty set.
            pub const NONE: Self = Self(0);
            /// `bitfield_flag::all()`.
            pub const ALL: Self = Self(<$ty>::MAX);

            /// `bitfield_flag(bit_t)` — flag value for bit `n`
            /// (`n_bit` literal upstream).
            #[must_use]
            pub const fn bit(n: u8) -> Self {
                Self(1 << n)
            }

            /// `explicit bitfield_flag(UnderlyingType)`.
            #[must_use]
            pub const fn from_bits(bits: $ty) -> Self {
                Self(bits)
            }

            /// `operator UnderlyingType`.
            #[must_use]
            pub const fn bits(self) -> $ty {
                self.0
            }

            /// `explicit operator bool` — any bit set?
            #[must_use]
            pub const fn is_set(self) -> bool {
                self.0 != 0
            }

            /// `(self & f) != 0` — the common `if (flags & flag)` test.
            #[must_use]
            pub const fn contains(self, f: Self) -> bool {
                self.0 & f.0 != 0
            }
        }

        impl std::ops::BitOr for $name {
            type Output = Self;
            fn bitor(self, rhs: Self) -> Self {
                Self(self.0 | rhs.0)
            }
        }
        impl std::ops::BitAnd for $name {
            type Output = Self;
            fn bitand(self, rhs: Self) -> Self {
                Self(self.0 & rhs.0)
            }
        }
        impl std::ops::BitXor for $name {
            type Output = Self;
            fn bitxor(self, rhs: Self) -> Self {
                Self(self.0 ^ rhs.0)
            }
        }
        impl std::ops::Not for $name {
            type Output = Self;
            fn not(self) -> Self {
                Self(!self.0)
            }
        }
        impl std::ops::BitOrAssign for $name {
            fn bitor_assign(&mut self, rhs: Self) {
                self.0 |= rhs.0;
            }
        }
        impl std::ops::BitAndAssign for $name {
            fn bitand_assign(&mut self, rhs: Self) {
                self.0 &= rhs.0;
            }
        }
        impl std::ops::BitXorAssign for $name {
            fn bitxor_assign(&mut self, rhs: Self) {
                self.0 ^= rhs.0;
            }
        }
        impl std::fmt::LowerHex for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::LowerHex::fmt(&self.0, f)
            }
        }
    };
}

bitfield_flag!(
    /// `torrent_flags_t` — per-torrent behavior flags (`torrent_flags.hpp`).
    /// Bit positions are part of the wire/resume format.
    TorrentFlags, u64
);

impl TorrentFlags {
    /// `torrent_flags::seed_mode` (0_bit) — seed, don't verify files.
    pub const SEED_MODE: Self = Self::bit(0);
    /// `torrent_flags::upload_mode` (1_bit) — never request pieces.
    pub const UPLOAD_MODE: Self = Self::bit(1);
    /// `torrent_flags::share_mode` (2_bit) — only download to keep ratio.
    pub const SHARE_MODE: Self = Self::bit(2);
    /// `torrent_flags::apply_ip_filter` (3_bit) — on by default.
    pub const APPLY_IP_FILTER: Self = Self::bit(3);
    /// `torrent_flags::paused` (4_bit).
    pub const PAUSED: Self = Self::bit(4);
    /// `torrent_flags::auto_managed` (5_bit).
    pub const AUTO_MANAGED: Self = Self::bit(5);
    /// `torrent_flags::duplicate_is_error` (6_bit) — not saved to resume data.
    pub const DUPLICATE_IS_ERROR: Self = Self::bit(6);
    /// `torrent_flags::update_subscribe` (7_bit) — not saved to resume data.
    pub const UPDATE_SUBSCRIBE: Self = Self::bit(7);
    /// `torrent_flags::super_seeding` (8_bit).
    pub const SUPER_SEEDING: Self = Self::bit(8);
    /// `torrent_flags::sequential_download` (9_bit).
    pub const SEQUENTIAL_DOWNLOAD: Self = Self::bit(9);
    /// `torrent_flags::stop_when_ready` (10_bit).
    pub const STOP_WHEN_READY: Self = Self::bit(10);
    /// `torrent_flags::deprecated_override_trackers` (11_bit) — hidden.
    pub const DEPRECATED_OVERRIDE_TRACKERS: Self = Self::bit(11);
    /// `torrent_flags::deprecated_override_web_seeds` (12_bit) — hidden.
    pub const DEPRECATED_OVERRIDE_WEB_SEEDS: Self = Self::bit(12);
    /// `torrent_flags::need_save_resume` (13_bit) — ephemeral, not saved.
    pub const NEED_SAVE_RESUME: Self = Self::bit(13);
    // bits 14..=18 were ABI-1 only (pinned, override_resume_data,
    // merge_resume_trackers, use_resume_save_path, merge_resume_http_seeds)
    // — upstream removed them; the gap is intentional.
    /// `torrent_flags::disable_dht` (19_bit).
    pub const DISABLE_DHT: Self = Self::bit(19);
    /// `torrent_flags::disable_lsd` (20_bit).
    pub const DISABLE_LSD: Self = Self::bit(20);
    /// `torrent_flags::disable_pex` (21_bit).
    pub const DISABLE_PEX: Self = Self::bit(21);
    /// `torrent_flags::no_verify_files` (22_bit) — trust resume data blindly.
    pub const NO_VERIFY_FILES: Self = Self::bit(22);
    /// `torrent_flags::default_dont_download` (23_bit).
    pub const DEFAULT_DONT_DOWNLOAD: Self = Self::bit(23);
    /// `torrent_flags::i2p_torrent` (24_bit).
    pub const I2P_TORRENT: Self = Self::bit(24);
    /// `torrent_flags::disable_v1_hashes` (25_bit) — hybrid torrents only
    /// verify v2 (SHA-256) piece hashes.
    pub const DISABLE_V1_HASHES: Self = Self::bit(25);

    /// `torrent_flags::default_flags` — the ABI≥2 set upstream applies to a
    /// fresh `add_torrent_params`.
    pub const DEFAULT_FLAGS: Self = Self(
        Self::UPDATE_SUBSCRIBE.0
            | Self::AUTO_MANAGED.0
            | Self::PAUSED.0
            | Self::APPLY_IP_FILTER.0
            | Self::NEED_SAVE_RESUME.0,
    );
}

bitfield_flag!(
    /// `pex_flags_t` — per-peer capabilities exchanged over PEX
    /// (`pex_flags.hpp`).
    PexFlags, u8
);

impl PexFlags {
    /// `pex_encryption` (0_bit) — peer supports protocol encryption.
    pub const ENCRYPTION: Self = Self::bit(0);
    /// `pex_seed` (1_bit) — peer is a seed.
    pub const SEED: Self = Self::bit(1);
    /// `pex_utp` (2_bit) — peer supports uTP.
    pub const UTP: Self = Self::bit(2);
    /// `pex_holepunch` (3_bit) — peer supports the holepunch extension.
    pub const HOLEPUNCH: Self = Self::bit(3);
    /// `pex_lt_v2` (7_bit) — protocol v2, internal only.
    pub const LT_V2: Self = Self::bit(7);
}

bitfield_flag!(
    /// `peer_source_flags_t` — where a peer was learned from
    /// (`peer_info.hpp`).
    PeerSourceFlags, u8
);

impl PeerSourceFlags {
    /// `peer_source_flags::tracker` (0_bit).
    pub const TRACKER: Self = Self::bit(0);
    /// `peer_source_flags::dht` (1_bit).
    pub const DHT: Self = Self::bit(1);
    /// `peer_source_flags::pex` (2_bit).
    pub const PEX: Self = Self::bit(2);
    /// `peer_source_flags::lsd` (3_bit).
    pub const LSD: Self = Self::bit(3);
    /// `peer_source_flags::resume_data` (4_bit).
    pub const RESUME_DATA: Self = Self::bit(4);
    /// `peer_source_flags::incoming` (5_bit) — no advertised listen port.
    pub const INCOMING: Self = Self::bit(5);
}

bitfield_flag!(
    /// `dht::announce_flags_t` — flags on a DHT announce
    /// (`kademlia/announce_flags.hpp`).
    AnnounceFlags, u8
);

impl AnnounceFlags {
    /// `dht::announce::seed` (0_bit) — announce as a seed.
    pub const SEED: Self = Self::bit(0);
    /// `dht::announce::implied_port` (1_bit) — use source UDP port.
    pub const IMPLIED_PORT: Self = Self::bit(1);
    /// `dht::announce::ssl_torrent` (2_bit) — port is for SSL listen socket.
    pub const SSL_TORRENT: Self = Self::bit(2);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_ops() {
        let f = TorrentFlags::PAUSED | TorrentFlags::AUTO_MANAGED;
        assert!(f.contains(TorrentFlags::PAUSED));
        assert!(f.contains(TorrentFlags::AUTO_MANAGED));
        assert!(!f.contains(TorrentFlags::SEED_MODE));
        assert!(f.is_set());
        assert!(!TorrentFlags::NONE.is_set());

        let cleared = f & !TorrentFlags::PAUSED;
        assert!(!cleared.contains(TorrentFlags::PAUSED));
        assert!(cleared.contains(TorrentFlags::AUTO_MANAGED));

        assert_eq!(TorrentFlags::SEED_MODE.bits(), 1);
        assert_eq!(TorrentFlags::DISABLE_V1_HASHES.bits(), 1 << 25);
    }

    #[test]
    fn default_flags_match_upstream() {
        let f = TorrentFlags::DEFAULT_FLAGS;
        assert!(f.contains(TorrentFlags::UPDATE_SUBSCRIBE));
        assert!(f.contains(TorrentFlags::AUTO_MANAGED));
        assert!(f.contains(TorrentFlags::PAUSED));
        assert!(f.contains(TorrentFlags::APPLY_IP_FILTER));
        assert!(f.contains(TorrentFlags::NEED_SAVE_RESUME));
        assert_eq!(
            f.bits(),
            (1 << 13) | (1 << 7) | (1 << 5) | (1 << 4) | (1 << 3)
        );
    }

    #[test]
    fn pex_and_announce() {
        assert_eq!(PexFlags::LT_V2.bits(), 0x80);
        assert_eq!(
            (AnnounceFlags::SEED | AnnounceFlags::IMPLIED_PORT).bits(),
            3
        );
    }
}
