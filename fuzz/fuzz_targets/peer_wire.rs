#![no_main]

use libfuzzer_sys::fuzz_target;

// Handshake + message parsing must reject garbage cleanly — peers are
// untrusted input by definition.
fuzz_target!(|data: &[u8]| {
    use bitdaemon_net::peer_wire;
    if let Ok(hs) = peer_wire::parse_handshake(data) {
        let _ = peer_wire::write_handshake(
            &hs.info_hash,
            &hs.peer_id,
            hs.supports_dht(),
            hs.supports_extension(),
            hs.supports_fast(),
            hs.supports_v2(),
        );
    }
    if data.len() >= 4 {
        let len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() >= 4 + len {
            let _ = peer_wire::decode(&data[4..4 + len]);
        }
    }
});
