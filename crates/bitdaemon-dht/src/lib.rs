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
//! Status: **not ported yet** — see `docs/PORTING.md`.

#![forbid(unsafe_code)]

/// DHT node id, 160-bit Kademlia distance space
/// (`libtorrent::dht::node_id` ≡ `sha1_hash`).
///
/// TODO: implement — `distance()` XOR metric, `generate` with
/// BEP-42 node-id verification for security.
pub struct NodeId;
