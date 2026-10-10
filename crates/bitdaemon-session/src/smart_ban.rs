//! Port of `extensions/smart_ban.cpp` — per-block CRC bookkeeping
//! used to ban peers that send corrupt data. The disk reads upstream
//! triggers are caller-driven here: the owner feeds block digests via
//! [`SmartBan::record_failed_block`] / [`SmartBan::check_ok_block`]
//! after its async reads complete. Decisions are returned as
//! [`BanDecision`] events instead of calling `torrent::ban_peer`
//! directly.

use std::collections::BTreeMap;
use std::net::IpAddr;

use bitdaemon_common::hash::Sha1Hash;
use bitdaemon_common::types::PieceIndex;

pub use bitdaemon_common::types::PieceBlock;

/// `block_entry` — ties a block CRC to the peer that sent it.
#[derive(Debug, Clone)]
struct BlockEntry {
    /// peer identity (`aux::torrent_peer*` upstream → our handle +
    /// address; the address is used to resolve churn like
    /// `find_peers(a)`).
    peer: u64,
    /// kept for `find_peers(a)`-style resolution on reconnect
    #[allow(dead_code)]
    peer_addr: IpAddr,
    digest: Sha1Hash,
}

/// What the owner should do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BanDecision {
    /// `m_torrent.ban_peer(p)` + disconnect — peer handle.
    Ban(u64),
    /// nothing.
    None,
}

/// `smart_ban_plugin` state.
#[derive(Debug, Default)]
pub struct SmartBan {
    /// `m_block_hashes`.
    block_hashes: BTreeMap<PieceBlock, BlockEntry>,
}

impl SmartBan {
    /// `smart_ban_plugin(torrent&)`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `on_piece_failed` bookkeeping — for each downloader of `p`,
    /// record `(block → (peer, crc))`. `read_digest` supplies the
    /// block's SHA-1 from storage (the caller performs the disk read;
    /// `None` = read error → skip, like `if (error) return`).
    ///
    /// `downloaders` is `picker().get_downloaders(p)` output; `addr`
    /// resolves a handle to its address.
    pub fn on_piece_failed(
        &mut self,
        p: PieceIndex,
        piece_size: i64,
        downloaders: &[Option<u64>],
        mut addr: impl FnMut(u64) -> IpAddr,
        mut read_digest: impl FnMut(PieceBlock) -> Option<Sha1Hash>,
    ) -> BanDecision {
        let mut decision = BanDecision::None;
        let mut size = piece_size;
        for (block, d) in downloaders.iter().enumerate() {
            if size <= 0 {
                break;
            }
            if let Some(peer) = d {
                let pb = PieceBlock {
                    piece_index: p,
                    block_index: block as i32,
                };
                if let Some(digest) = read_digest(pb) {
                    if let BanDecision::Ban(h) =
                        self.record_failed_block(pb, *peer, addr(*peer), digest)
                    {
                        decision = BanDecision::Ban(h);
                    }
                }
            }
            size -= 16 * 1024;
        }
        decision
    }

    /// `on_read_failed_block` — store/confirm a block CRC for `peer`.
    /// Ban when the same peer gives a *different* CRC for the same
    /// block on the second failed piece.
    pub fn record_failed_block(
        &mut self,
        b: PieceBlock,
        peer: u64,
        peer_addr: IpAddr,
        digest: Sha1Hash,
    ) -> BanDecision {
        match self.block_hashes.get(&b) {
            Some(e) if e.peer == peer => {
                // already have an entry from this peer for this block:
                // differing digest means at least one of them was bad
                if e.digest != digest {
                    return BanDecision::Ban(peer);
                }
                BanDecision::None
            }
            _ => {
                self.block_hashes.insert(
                    b,
                    BlockEntry {
                        peer,
                        peer_addr,
                        digest,
                    },
                );
                BanDecision::None
            }
        }
    }

    /// `on_piece_passed` — the piece passed its hash this time; every
    /// recorded block whose stored CRC now mismatches incriminates its
    /// sender. `read_digest` is the post-success read.
    pub fn on_piece_passed(
        &mut self,
        p: PieceIndex,
        piece_size: i64,
        mut read_digest: impl FnMut(PieceBlock) -> Option<Sha1Hash>,
    ) -> Vec<BanDecision> {
        // split the entries for piece p (drain the map range)
        let mut entries: Vec<(PieceBlock, BlockEntry)> = Vec::new();
        let keys: Vec<PieceBlock> = self
            .block_hashes
            .range(
                PieceBlock {
                    piece_index: p,
                    block_index: 0,
                }..PieceBlock {
                    piece_index: PieceIndex(p.0 + 1),
                    block_index: 0,
                },
            )
            .map(|(k, _)| *k)
            .collect();
        for k in keys {
            if let Some(e) = self.block_hashes.remove(&k) {
                entries.push((k, e));
            }
        }

        let mut out = Vec::new();
        for (k, e) in entries {
            debug_assert_eq!(k.piece_index, p);
            // upstream: re-read the block and compare — a stored CRC
            // that differs from the now-correct block incriminates
            // the peer that sent the bad copy
            if let Some(ok) = read_digest(k) {
                if ok != e.digest {
                    out.push(BanDecision::Ban(e.peer));
                }
            }
            // (read errors are ignored upstream — `if (error) return`)
        }
        let _ = piece_size; // block enumeration is driven by the map
        out
    }

    /// `on_erase_peers` — drop entries whose peer was erased.
    pub fn erase_peers(&mut self, peers: &[u64]) {
        if peers.is_empty() {
            return;
        }
        self.block_hashes.retain(|_, e| !peers.contains(&e.peer));
    }

    /// `on_clear_peers` — the whole peer list was torn down.
    pub fn clear(&mut self) {
        self.block_hashes.clear();
    }
}
