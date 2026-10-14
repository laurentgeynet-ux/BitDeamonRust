//! `fingerprint.cpp` — the `lt::fingerprint` version tuple and
//! `generate_fingerprint` (`-XX1234-` peer-id prefixes).
//!
//! `identify_client.cpp` (peer-id → display name) lives in
//! `bitdaemon_common::identify_client` — the canonical implementation,
//! re-exported here so `fingerprint::identify_client` keeps working.
//! Sharing the single copy (tables, parsers, signed-char semantics)
//! avoids the two implementations drifting apart.

use bitdaemon_common::PeerId;

/// `lt::fingerprint` — `-XX1234-` style client id + version digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fingerprint {
    /// Two-letter client code (second byte may be 0 for one-letter ids).
    pub name: [u8; 2],
    /// `major_version`.
    pub major_version: i32,
    /// `minor_version`.
    pub minor_version: i32,
    /// `revision_version`.
    pub revision_version: i32,
    /// `tag_version`.
    pub tag_version: i32,
}

impl Fingerprint {
    /// `generate_fingerprint` — format `-{name}{major}{minor}{rev}{tag}-`.
    ///
    /// `name` is truncated to 2 chars, or right-padded with `-` if shorter.
    #[must_use]
    pub fn generate(name: &[u8], major: i32, minor: i32, revision: i32, tag: i32) -> Vec<u8> {
        debug_assert!(major >= 0 && minor >= 0 && revision >= 0 && tag >= 0);
        fn v2c(v: i32) -> u8 {
            if (0..10).contains(&v) {
                b'0' + v as u8
            } else {
                b'A' + (v - 10) as u8
            }
        }
        vec![
            b'-',
            name.first().copied().unwrap_or(b'-'),
            name.get(1).copied().unwrap_or(b'-'),
            v2c(major),
            v2c(minor),
            v2c(revision),
            v2c(tag),
            b'-',
        ]
    }
}

/// `lt::identify_client` — human-readable client name for a peer id.
///
/// Delegates to [`bitdaemon_common::identify_client::identify_client`],
/// the canonical port of `aux::identify_client_impl`.
#[must_use]
pub fn identify_client(p: &PeerId) -> String {
    bitdaemon_common::identify_client::identify_client(p)
}
