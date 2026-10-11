#![no_main]

use libfuzzer_sys::fuzz_target;

// Upstream fuzzes bdecode via test/test_bdecode.cpp corpus; the decoder
// must never panic and must print values it accepts.
fuzz_target!(|data: &[u8]| {
    if let Ok(n) = bitdaemon_bencode::bdecode_checked(data) {
        let _ = bitdaemon_bencode::print_node(&n, true, 0);
    }
});
