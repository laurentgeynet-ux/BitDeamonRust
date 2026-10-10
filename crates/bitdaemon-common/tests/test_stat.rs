//! `stat.cpp`/`stat.hpp` behavior checks — upstream has no dedicated test
//! file; these verify the transcribed formulas (tick math, SYN costs, IP
//! overhead, channel aggregation).

use bitdaemon_common::stat::{Stat, StatChannel, StatChannelKind};

#[test]
fn channel_tick_math() {
    let mut c = StatChannel::default();
    c.add(1000);
    assert_eq!(c.counter(), 1000);
    assert_eq!(c.total(), 1000);

    // 1000 bytes over a 1 s tick → sample 1000, avg = 0*4/5 + 1000/5 = 200
    c.second_tick(1000);
    assert_eq!(c.counter(), 0);
    assert_eq!(c.rate(), 200);
    assert_eq!(c.low_pass_rate(), 200);

    // idle tick decays: avg = 200*4/5 + 0 = 160
    c.second_tick(1000);
    assert_eq!(c.rate(), 160);

    // half-second tick: sample doubles (rate is per-second)
    c.add(500);
    c.second_tick(500);
    // sample = 500*1000/500 = 1000 → avg = 160*4/5 + 1000/5 = 128 + 200 = 328
    assert_eq!(c.rate(), 328);
}

#[test]
fn offset_and_clear() {
    let mut c = StatChannel::default();
    c.offset(5000);
    assert_eq!(c.total(), 5000);
    assert_eq!(c.counter(), 0);
    c.add(10);
    c.clear();
    assert_eq!((c.counter(), c.total(), c.rate()), (0, 0, 0));
}

#[test]
fn syn_accounting() {
    let mut s = Stat::default();
    s.sent_syn(false);
    assert_eq!(s.upload_ip_overhead(), 40);
    s.sent_syn(true);
    assert_eq!(s.upload_ip_overhead(), 100);

    s.received_synack(false);
    assert_eq!(s.download_ip_overhead(), 40);
    assert_eq!(s.upload_ip_overhead(), 140);
}

#[test]
fn ip_packet_overhead() {
    let mut s = Stat::default();
    // one packet's worth of payload: ceil(1460/1460)=1 → 40 B overhead
    s.trancieve_ip_packet(1460, false);
    assert_eq!(s.download_ip_overhead(), 40);
    assert_eq!(s.upload_ip_overhead(), 40);

    // two packets: ceil(1461/1460)=2 → 80 B
    s.trancieve_ip_packet(1461, false);
    assert_eq!(s.download_ip_overhead(), 120);

    // ipv6: header 60, packet_size 1440; 1440 → 60 B
    let mut s6 = Stat::default();
    s6.trancieve_ip_packet(1440, true);
    assert_eq!(s6.download_ip_overhead(), 60);

    // zero bytes still costs one header (max(1, ...))
    let mut s0 = Stat::default();
    s0.trancieve_ip_packet(0, false);
    assert_eq!(s0.download_ip_overhead(), 40);
}

#[test]
fn channel_aggregation() {
    let mut s = Stat::default();
    s.sent_bytes(100, 20);
    s.received_bytes(50, 10);
    assert_eq!(s.total_payload_upload(), 100);
    assert_eq!(s.total_payload_download(), 50);
    assert_eq!(s.total_protocol_upload(), 20);
    assert_eq!(s.total_protocol_download(), 10);
    assert_eq!(s.total_upload(), 120);
    assert_eq!(s.total_download(), 60);
    assert_eq!(s.last_payload_uploaded(), 100);
    assert_eq!(s.last_protocol_downloaded(), 10);
    assert_eq!(s.total_transfer(StatChannelKind::UploadPayload), 100);

    s.add_stat(7_000_000, 3_000_000);
    assert_eq!(s.total_payload_download(), 7_000_050);
    assert_eq!(s.total_payload_upload(), 3_000_100);
    // offset doesn't touch the rate window
    assert_eq!(s.last_payload_downloaded(), 50);
}
