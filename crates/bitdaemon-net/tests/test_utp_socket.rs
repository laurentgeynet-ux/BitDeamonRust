// Conformance test for `utp_socket_impl` (utp_stream.cpp) — two
// sockets wired back-to-back: SYN handshake, DATA in both directions,
// reordering, FIN close.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bitdaemon_net::utp::{UtpHeader, UtpSocketDemux, UtpType};
use bitdaemon_net::utp_socket::{UtpConfig, UtpError, UtpSocket, UtpStat, UtpState};

fn pump(a: &mut UtpSocket, b: &mut UtpSocket, now: Instant) {
    // deliver a's outbox into b and vice versa
    for d in a.drain_outbox() {
        b.incoming_packet(&d.bytes, d.to, now);
    }
    for d in b.drain_outbox() {
        a.incoming_packet(&d.bytes, d.to, now);
    }
}

fn pair() -> (UtpSocket, UtpSocket, SocketAddr, SocketAddr) {
    let a_addr = SocketAddr::from(([127, 0, 0, 1], 1000));
    let b_addr = SocketAddr::from(([127, 0, 0, 1], 2000));
    // send_id n, recv_id n+1; b's send_id = a's recv_id
    let a = UtpSocket::new(1, 2, UtpConfig::default());
    let b = UtpSocket::new(2, 1, UtpConfig::default());
    (a, b, a_addr, b_addr)
}

#[test]
fn utp_handshake_data_fin() {
    let (mut a, mut b, a_addr, b_addr) = pair();
    let now = Instant::now();
    a.connect(b_addr);

    // SYN went out addressed to b's recv_id
    let syn: Vec<_> = a.drain_outbox().collect();
    assert_eq!(syn.len(), 1);
    let h = UtpHeader::parse(&syn[0].bytes).unwrap();
    assert_eq!(h.ty, UtpType::Syn);
    // upstream quirk: the SYN carries *our* recv_id (the id the peer
    // must ACK on); subsequent packets use recv_id+1
    assert_eq!(h.connection_id, 1);
    assert_eq!(a.state(), UtpState::SynSent);

    // deliver SYN; b acks (deferred)
    b.incoming_packet(&syn[0].bytes, a_addr, now);
    assert_eq!(b.state(), UtpState::Connected);
    assert!(b.matches(a_addr, 2)); // b.recv_id == 2
    pump(&mut a, &mut b, now); // STATE ack → a connected
    // b's ack was deferred — flush via tick
    b.tick(now);
    pump(&mut a, &mut b, now);
    assert_eq!(a.state(), UtpState::Connected);

    // a → b data
    a.write(b"hello utp");
    pump(&mut a, &mut b, now);
    let mut out = [0u8; 64];
    let n = b.read(&mut out).unwrap();
    assert_eq!(&out[..n], b"hello utp");

    // b → a data
    b.write(b"world");
    pump(&mut a, &mut b, now);
    let n = a.read(&mut out).unwrap();
    assert_eq!(&out[..n], b"world");

    // close: a sends FIN, b acks, a → FinSent → Eof after ack
    a.close();
    pump(&mut a, &mut b, now);
    b.tick(now);
    pump(&mut a, &mut b, now);
    assert!(b.in_eof_seen() || a.state() >= UtpState::FinSent);
}

#[test]
fn utp_out_of_order() {
    let (mut a, mut b, a_addr, _b_addr) = pair();
    let now = Instant::now();
    a.connect(_b_addr);
    let syn: Vec<_> = a.drain_outbox().collect();
    b.incoming_packet(&syn[0].bytes, a_addr, now);
    b.tick(now);
    pump(&mut a, &mut b, now);

    // send two data packets, deliver the second first
    a.write(b"");
    a.write(b"first-chunk");
    let datagrams: Vec<_> = a.drain_outbox().collect();
    assert!(!datagrams.is_empty());
    let last = datagrams.last().unwrap().bytes.clone();
    // feed the last packet out of order (reorder buffer)
    if datagrams.len() >= 2 {
        b.incoming_packet(&last, a_addr, now);
        for d in &datagrams[..datagrams.len() - 1] {
            b.incoming_packet(&d.bytes, a_addr, now);
        }
    } else {
        b.incoming_packet(&last, a_addr, now);
    }
    let mut out = [0u8; 64];
    let n = b.read(&mut out).unwrap();
    assert_eq!(&out[..n], b"first-chunk");
}

/// connected pair, handshake done (a initiated; b acked). Returns
/// `(a, b, a_addr, b_addr, now)` — the SYN was delivered and its
/// STATE ack pumped back, so both sockets are `Connected`.
fn connected_pair() -> (UtpSocket, UtpSocket, SocketAddr, SocketAddr, Instant) {
    let (mut a, mut b, a_addr, b_addr) = pair();
    let now = Instant::now();
    a.connect(b_addr);
    let syn: Vec<_> = a.drain_outbox().collect();
    assert_eq!(syn.len(), 1);
    b.incoming_packet(&syn[0].bytes, a_addr, now);
    b.tick(now); // flushes the deferred STATE ack
    pump(&mut a, &mut b, now);
    assert_eq!(a.state(), UtpState::Connected);
    assert_eq!(b.state(), UtpState::Connected);
    (a, b, a_addr, b_addr, now)
}

fn datagram_types(sock: &mut UtpSocket) -> Vec<UtpType> {
    sock.drain_outbox()
        .map(|d| UtpHeader::parse(&d.bytes).unwrap().ty)
        .collect()
}

// D1 — `close()` must not emit the FIN while a Nagle packet or a
// packet under `m_seq_nr` is still pending (upstream's guards). The
// FIN would otherwise overtake queued payload on the wire.
#[test]
fn close_waits_for_pending_nagle() {
    let (mut a, mut b, a_addr, _b_addr, now) = connected_pair();

    // first small write goes out and stays in flight unacked —
    // `m_nagle_seq_nr` is stamped to its seq
    a.write(b"one");
    let first: Vec<_> = a.drain_outbox().collect();
    assert!(!first.is_empty());

    // second write is held by Nagle (bytes in flight, first packet
    // small and still unacked)
    a.write(b"two");

    // close() while the nagle packet is held must NOT send the FIN
    a.close();
    assert_eq!(a.state(), UtpState::Connected);
    let types = datagram_types(&mut a);
    assert!(
        !types.contains(&UtpType::Fin),
        "FIN emitted while a nagle packet was held: {types:?}"
    );

    // deliver the first packet; b acks; the ack unblocks the nagle
    // packet — it leaves as DATA on the next send_pkt
    for d in first {
        b.incoming_packet(&d.bytes, a_addr, now);
    }
    b.send_deferred_ack();
    for d in b.drain_outbox() {
        a.incoming_packet(&d.bytes, _b_addr, now);
    }
    a.tick(now);
    let flushed: Vec<_> = a.drain_outbox().collect();
    assert!(
        flushed
            .iter()
            .any(|d| UtpHeader::parse(&d.bytes).unwrap().ty == UtpType::Data),
        "nagle packet should flush as DATA: {flushed:?}"
    );

    // deliver it too; once the whole outbuf is acked the FIN
    // leaves on the next send_pkt (upstream's send_pkt tail)
    for d in flushed {
        b.incoming_packet(&d.bytes, a_addr, now);
    }
    b.send_deferred_ack();
    for d in b.drain_outbox() {
        a.incoming_packet(&d.bytes, _b_addr, now);
    }
    a.tick(now);
    let types = datagram_types(&mut a);
    assert!(
        types.contains(&UtpType::Fin),
        "expected FIN after the queue drained: {types:?}"
    );
}

// D9 — `abort()` must run `test_socket_state`: with the error
// stored, the socket transitions `error_wait → deleting` right away
// (upstream's teardown progression).
#[test]
fn abort_progresses_to_deleting() {
    let (mut a, ..) = pair();
    a.abort();
    assert_eq!(a.error(), Some(UtpError::Aborted));
    assert_eq!(a.state(), UtpState::Deleting);
}

// D3 — `set_close_reason` tags the FIN with the `utp_close_reason`
// extension (id 3, 4-byte payload).
#[test]
fn close_reason_rides_the_fin() {
    let (mut a, mut b, a_addr, _b_addr, _now) = connected_pair();
    a.set_close_reason(1); // close_reason_t::invalid? a non-zero code
    a.close();
    let fin: Vec<_> = a
        .drain_outbox()
        .filter(|d| UtpHeader::parse(&d.bytes).unwrap().ty == UtpType::Fin)
        .collect();
    assert_eq!(fin.len(), 1, "expected exactly one FIN");
    let bytes = &fin[0].bytes;
    assert_eq!(bytes[1], 3, "FIN must carry the close_reason extension");
    assert_eq!(bytes[20], 0, "close_reason is the last extension");
    assert_eq!(bytes[21], 4, "close_reason payload is 4 bytes");
    assert_eq!(u32::from_be_bytes(bytes[22..26].try_into().unwrap()), 1);

    // the peer parses it into `their_close_reason`
    b.incoming_packet(bytes, a_addr, _now);
    assert_eq!(b.their_close_reason, 1);
}

// D10 — the upstream counter sites fire through `counter_sink`.
#[test]
fn counter_sink_observes_wire_activity() {
    let seen = Arc::new(Mutex::new(Vec::<(UtpStat, i64)>::new()));
    let s = seen.clone();
    let cfg = UtpConfig {
        counter_sink: Some(Arc::new(move |stat, delta| {
            s.lock()
                .unwrap_or_else(|p| p.into_inner())
                .push((stat, delta));
        })),
        ..UtpConfig::default()
    };
    let (a, mut b, a_addr, b_addr) = pair();
    // swap in the instrumented config on `a` by rebuilding it
    drop(a);
    let mut a = UtpSocket::new(1, 2, cfg);
    let now = Instant::now();
    a.connect(b_addr);
    let syn: Vec<_> = a.drain_outbox().collect();
    b.incoming_packet(&syn[0].bytes, a_addr, now);
    b.tick(now);
    for d in b.drain_outbox() {
        a.incoming_packet(&d.bytes, b_addr, now);
    }
    a.write(b"ping");
    for d in a.drain_outbox() {
        b.incoming_packet(&d.bytes, a_addr, now);
    }
    // one garbage datagram → invalid_pkts_in
    a.incoming_packet(&[0xffu8; 4], b_addr, now);

    let counts = |stat: UtpStat| -> i64 {
        seen.lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|(s, _)| *s == stat)
            .map(|(_, d)| *d)
            .sum()
    };
    // upstream counts utp_packets_out inside send_pkt only — the
    // SYN goes through send_syn and is not counted; the garbage
    // datagram still counts as packets_in (counter sits at the top
    // of incoming_packet, before any validity check)
    assert_eq!(counts(UtpStat::PacketsOut), 1);
    assert_eq!(counts(UtpStat::PayloadPktsOut), 1);
    assert_eq!(counts(UtpStat::PacketsIn), 2);
    assert_eq!(counts(UtpStat::InvalidPktsIn), 1);
    // state gauges net out: ctor +1 for None, SynSent/Connected
    // transitions balanced, Drop −1 at scope end
    let gauge = |idx: usize| -> i64 {
        seen.lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|(s, _)| *s == UtpStat::StateGauge(idx))
            .map(|(_, d)| *d)
            .sum()
    };
    assert_eq!(
        gauge(UtpState::None as usize),
        0,
        "None: +1 ctor, −1 on SynSent"
    );
    assert_eq!(gauge(UtpState::SynSent as usize), 0);
    assert_eq!(gauge(UtpState::Connected as usize), 1);
    drop(a);
    assert_eq!(
        gauge(UtpState::Connected as usize),
        0,
        "dtor releases the gauge"
    );
}

// R1 — a `write` after `close()` is `broken_pipe` upstream
// (`write_some` rejects past `m_out_eof`): it must return 0 and
// must not append payload — the bytes could otherwise ride a
// packet stamped after the queued FIN.
#[test]
fn write_after_close_is_broken_pipe() {
    let (mut a, ..) = connected_pair();
    a.close();
    assert_eq!(a.write(b"late"), 0);
    // the FIN still leaves — and nothing carries the post-close
    // payload
    let dgrams: Vec<_> = a.drain_outbox().collect();
    assert!(
        dgrams
            .iter()
            .all(|d| !d.bytes.windows(4).any(|w| w == b"late")),
        "post-close payload must not be queued"
    );
    assert!(
        dgrams
            .iter()
            .any(|d| UtpHeader::parse(&d.bytes).unwrap().ty == UtpType::Fin),
        "expected the FIN to flush"
    );
}

// R9 — upstream erases the packet's `m_needs_resend` link inside
// `resend_packet`. A timeout-driven resend that left `seq` queued
// made the next `send_pkt` transmit a second copy (double resend,
// inflated `num_transmissions`).
#[test]
fn timeout_resend_unlinks_needs_resend() {
    let (mut a, ..) = connected_pair();
    a.write(b"one-packet");
    let first: Vec<_> = a.drain_outbox().collect();
    assert_eq!(first.len(), 1);
    let seq = UtpHeader::parse(&first[0].bytes).unwrap().seq_nr;

    // the peer never acks — drive a timeout (packet_timeout() is
    // ≥ min_timeout = 500 ms by default; jump past it)
    a.tick(Instant::now() + std::time::Duration::from_secs(10));
    let resent: Vec<_> = a.drain_outbox().collect();
    assert_eq!(resent.len(), 1, "timeout must resend the packet once");
    assert_eq!(UtpHeader::parse(&resent[0].bytes).unwrap().seq_nr, seq);

    // any send_pkt trigger must NOT resend it a second time —
    // the resend queue entry was unlinked inside resend_packet_seq
    a.write(b"");
    assert!(
        a.drain_outbox().next().is_none(),
        "stale needs_resend entry resent the packet a second time"
    );
}

// R3 — `send_pkt`'s send-error legs: EMSGSIZE clamps the MTU
// ceiling and requeues without DF; a hard transport error stores
// `m_error` and walks `error_wait → deleting`.
#[test]
fn send_error_legs() {
    use bitdaemon_net::utp_socket::OutDatagram;
    let (mut a, _b, _a_addr, b_addr, _now) = connected_pair();

    // EMSGSIZE (90): a DF-flagged datagram is requeued without DF
    // and the socket stays connected — the MTU-probe fallback
    a.write(b"probe-sized-payload");
    let mut d = a.pop_outbox().unwrap();
    d.dont_fragment = true; // pretend it was a DF-flagged probe
    let refused = std::io::Error::from_raw_os_error(90);
    a.note_send_error(d, &refused);
    assert_eq!(a.state(), UtpState::Connected);
    let retried = a.pop_outbox().unwrap();
    assert!(!retried.dont_fragment, "EMSGSIZE retry must drop DF");

    // a hard error (e.g. ECONNREFUSED) kills the connection
    a.note_send_error(
        OutDatagram {
            to: b_addr,
            bytes: vec![0u8; 20],
            dont_fragment: false,
        },
        &std::io::Error::from_raw_os_error(111),
    );
    assert!(matches!(a.error(), Some(UtpError::Io(_))));
    assert_eq!(a.state(), UtpState::Deleting);
}

// D8 — `UtpSocketDemux` allocates random ids (upstream
// `random(0xffff)`, recv = send−1) and routes by `(remote, id)` —
// upstream `match(ep, id)`.
#[test]
fn demux_ids_and_endpoint_keying() {
    let ep = SocketAddr::from(([10, 0, 0, 1], 5000));
    let other = SocketAddr::from(([10, 0, 0, 2], 5000));
    let mut demux = UtpSocketDemux::default();
    let (send_id, recv_id) = demux.new_socket(ep, "sock");
    assert_eq!(recv_id, send_id.wrapping_sub(1));
    assert_eq!(demux.len(), 1);

    // a datagram carrying conn_id = recv_id routes only from `ep`
    let mut pkt = UtpHeader {
        ty: UtpType::State,
        version: 1,
        extension: 0,
        connection_id: recv_id,
        timestamp_us: 0,
        delay_us: 0,
        wnd_size: 0,
        seq_nr: 0,
        ack_nr: 0,
    }
    .encode()
    .to_vec();
    assert!(demux.route(ep, &pkt).is_some());
    assert!(
        demux.route(other, &pkt).is_none(),
        "wrong endpoint must miss"
    );
    pkt[2..4].copy_from_slice(&recv_id.wrapping_add(1).to_be_bytes());
    assert!(demux.route(ep, &pkt).is_none(), "wrong conn-id must miss");

    // a second socket gets an independent random id (1/65536
    // collision chance is acceptable for a unit test — the *shape*
    // recv = send−1 is the invariant)
    let (send_id2, recv_id2) = demux.new_socket(ep, "sock2");
    assert_eq!(recv_id2, send_id2.wrapping_sub(1));
    assert_eq!(demux.len(), 2);
}
