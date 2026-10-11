//! Golden-vector conformance for `write_session_params` /
//! `read_session_params` — fixtures produced by C++ libtorrent v2.1.2
//! (`tools/golden_vectors/generate.cpp`). The `.session` params dict
//! must be byte-identical.
use bitdaemon_bencode::{Entry, bdecode, bencode};
use bitdaemon_session::session_params::{
    read_session_params, write_session_params, write_session_params_buf,
};
use bitdaemon_session::settings::SessionParams;

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!("tests/data/{name}.bin")).expect("fixture")
}

/// Default params → identical dict to upstream (dht defaults,
/// empty dht state, empty settings, empty extensions, full-range
/// ip_filter4/6 defaults).
#[test]
fn golden_session_default() {
    let cpp = fixture("session_default");
    let ours = write_session_params_buf(&SessionParams::default());
    let cpp_e = Entry::from(&bdecode(&cpp));
    let our_e = Entry::from(&bdecode(&ours));
    assert_eq!(cpp_e, our_e, "decoded dicts differ");
    assert_eq!(ours, cpp, "bytes differ from upstream v2.1.2");
}

/// Custom settings round-trip: write with the same params the C++
/// generator used and byte-compare.
#[test]
fn golden_session_custom() {
    let cpp = fixture("session_custom");
    let mut sp = SessionParams::default();
    sp.settings.download_rate_limit = 12345;
    sp.settings.dht_enabled = false;
    sp.settings.user_agent = b"fixture/1.0".to_vec();
    let ours = write_session_params_buf(&sp);
    assert_eq!(
        Entry::from(&bdecode(&ours)),
        Entry::from(&bdecode(&cpp)),
        "decoded dicts differ"
    );
    assert_eq!(ours, cpp, "bytes differ from upstream v2.1.2");
}

/// Reading the upstream fixture yields the fields upstream wrote.
#[test]
fn golden_session_read() {
    let sp = read_session_params(&fixture("session_custom"));
    assert_eq!(sp.settings.download_rate_limit, 12345);
    assert!(!sp.settings.dht_enabled);
    assert_eq!(sp.settings.user_agent, b"fixture/1.0");
    // untouched fields stay at defaults
    assert_eq!(sp.settings.connections_limit, 200);
}

/// Round-trip: params -> bytes -> params.
#[test]
fn session_params_roundtrip() {
    let mut sp = SessionParams::default();
    sp.settings.upload_rate_limit = 777;
    sp.dht_settings.max_peers = 42;
    sp.ip_filter
        .add_rule("10.0.0.1".parse().unwrap(), "10.0.0.9".parse().unwrap(), 1);
    sp.ext_state.insert(b"myext".to_vec(), b"blob".to_vec());
    let back = read_session_params(&bencode(&write_session_params(&sp)));
    assert_eq!(back.settings.upload_rate_limit, 777);
    assert_eq!(back.dht_settings.max_peers, 42);
    assert_eq!(back.ip_filter.access("10.0.0.5".parse().unwrap()), 1);
    assert_eq!(
        back.ext_state.get(b"myext".as_slice()),
        Some(&b"blob".to_vec())
    );
}
