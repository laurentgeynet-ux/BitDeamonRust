//! Mainline DHT — port of `libtorrent::dht` (Kademlia, BEP-5/32/42/44).
//!
//! Ports `src/kademlia/*`: `node`, `routing_table`, `rpc_manager`,
//! `traversal_algorithm`, `find_data`, `get_peers`, `get_item`,
//! `put_data`, `refresh`, `sample_infohashes` (BEP-51), `dht_storage`,
//! `dht_state`, `dht_tracker`, `dos_blocker`, `node_id`, `node_entry`,
//! `msg`, `item`, `observer`.
//!
//! Ed25519 signing uses the `ed25519-dalek` crate instead of porting
//! the bundled C implementation — see ADR-0007.
//!
//! Status: `node_id` (BEP-42) ported — see `docs/PORTING.md`.

#![forbid(unsafe_code)]

pub mod dht_node;
pub mod dht_settings;
pub mod dht_state;
pub mod dht_storage;
pub mod dht_tracker;
pub mod dos_blocker;
pub mod ed25519;
pub mod item;
pub mod lookups;
pub mod msg;
pub mod node_entry;
pub mod node_id;
pub mod routing_table;
pub mod rpc;
pub mod traversal;

pub use node_id::NodeId;

/// `v` key stamped on every outgoing datagram — upstream writes
/// `{'L','T', major, minor<<4|tiny}` in `dht_tracker::send_packet`.
/// This is a distinct implementation, so it advertises `BD` +
/// the workspace version (0.1.0 → `\x00\x10`), matching the
/// upstream encoding scheme.
pub const CLIENT_VERSION: [u8; 4] = [b'B', b'D', 0, 0x10];
