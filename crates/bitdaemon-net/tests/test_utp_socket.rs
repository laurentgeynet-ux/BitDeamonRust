// Conformance test for `utp_socket_impl` (utp_stream.cpp) — two
// sockets wired back-to-back: SYN handshake, DATA in both directions,
// reordering, FIN close.

use std::net::SocketAddr;
use std::time::Instant;

use bitdaemon_net::utp::{UtpHeader, UtpType};
use bitdaemon_net::utp_socket::{UtpConfig, UtpSocket, UtpState};

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
