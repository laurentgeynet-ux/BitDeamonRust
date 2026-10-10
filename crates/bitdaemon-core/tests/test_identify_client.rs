//! Conformance port of `test_identify_client.cpp` + `fingerprint`
//! generation vectors.

use bitdaemon_common::PeerId;
use bitdaemon_core::fingerprint::{Fingerprint, identify_client};

fn pid(s: &[u8]) -> PeerId {
    let mut id = [0u8; 20];
    id[..s.len().min(20)].copy_from_slice(&s[..s.len().min(20)]);
    PeerId::from_bytes(&id).expect("valid peer id")
}

#[test]
fn identify_client_cases() {
    let cases: [(&[u8], &str); 14] = [
        (b"-AZ123B-............", "Azureus 1.2.3.11"),
        (b"-AZ1230-............", "Azureus 1.2.3"),
        (b"S123--..............", "Shadow 1.2.3"),
        (b"S\x01\x02\x03....\0...........", "Shadow 1.2.3"),
        (b"M1-2-3--............", "Mainline 1.2.3"),
        (b"\0\0\0\0\0\0\0\0\0\0\0\0........", "Generic"),
        (b"-xx1230-............", "xx 1.2.3"),
        (b"-TR2210-............", "Transmission 2.21.0"),
        (b"-TR2220-............", "Transmission 2.22.0"),
        (b"-TR2830-............", "Transmission 2.83.0"),
        (b"-TR3000-............", "Transmission 3.0.0"),
        (b"-TR300Z-............", "Transmission 3.0.0.35"),
        (b"-TR300X-............", "Transmission 3.0.0.33"),
        // non-digit, non-uppercase suffix is not a valid legacy
        // Transmission encoding
        (b"-TR300!-............", "Unknown [-TR300!-............]"),
    ];
    for (id, expect) in cases {
        assert_eq!(identify_client(&pid(id)), expect, "{id:?}");
    }
}

#[test]
fn generate_fingerprint() {
    // `-XXabcd-` — digits 0-9, >=10 → uppercase letters
    assert_eq!(
        Fingerprint::generate(b"UT", 1, 8, 1, 0),
        b"-UT1810-".to_vec()
    );
    assert_eq!(
        Fingerprint::generate(b"AZ", 3, 10, 0, 2),
        b"-AZ3A02-".to_vec()
    );
    // short client names pad with '-'
    assert_eq!(
        Fingerprint::generate(b"A", 1, 0, 0, 0),
        b"-A-1000-".to_vec()
    );
}
