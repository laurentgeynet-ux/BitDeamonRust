// SOCKS4/5 handshake tests — a scripted server over `tokio::io::duplex`
// validates the wire protocol byte-for-byte.

use bitdaemon_net::socks::{SocksCommand, SocksError, SocksTarget, socks_connect};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

#[tokio::test]
async fn socks5_noauth_connect() {
    let (mut client, mut server) = duplex(4096);
    let server_task = tokio::spawn(async move {
        // methods advertisement [5, 1, 0]
        let mut buf = [0u8; 3];
        server.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, [5, 1, 0]);
        server.write_all(&[5, 0]).await.unwrap();
        // CONNECT [5, 1, 0, 1, ip4, port]
        let mut req = [0u8; 10];
        server.read_exact(&mut req).await.unwrap();
        assert_eq!(req[0], 5);
        assert_eq!(req[1], 1); // CONNECT
        assert_eq!(req[3], 1); // IPv4
        assert_eq!(&req[4..8], &[127, 0, 0, 1]);
        assert_eq!(&req[8..10], &6881u16.to_be_bytes());
        // reply: success, bound 0.0.0.0:0
        server
            .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
            .await
            .unwrap();
    });

    let target = SocksTarget::Addr(SocketAddr::from(([127, 0, 0, 1], 6881)));
    socks_connect(&mut client, 5, "", "", &target, SocksCommand::Connect)
        .await
        .unwrap();
    server_task.await.unwrap();
}

#[tokio::test]
async fn socks5_userpass() {
    let (mut client, mut server) = duplex(4096);
    let server_task = tokio::spawn(async move {
        let mut buf = [0u8; 4];
        server.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, [5, 2, 0, 2]); // two methods offered
        server.write_all(&[5, 2]).await.unwrap(); // pick user/pass
        // [1, ulen, user, plen, pass]
        let mut a = [0u8; 2];
        server.read_exact(&mut a).await.unwrap();
        assert_eq!(a[0], 1);
        let mut u = vec![0u8; a[1] as usize];
        server.read_exact(&mut u).await.unwrap();
        assert_eq!(u, b"alice");
        let mut plen = [0u8; 1];
        server.read_exact(&mut plen).await.unwrap();
        let mut p = vec![0u8; plen[0] as usize];
        server.read_exact(&mut p).await.unwrap();
        assert_eq!(p, b"secret");
        server.write_all(&[1, 0]).await.unwrap();
        // CONNECT to a hostname
        let mut hdr = [0u8; 4];
        server.read_exact(&mut hdr).await.unwrap();
        assert_eq!(&hdr[..4], &[5, 1, 0, 3]);
        let mut n = [0u8; 1];
        server.read_exact(&mut n).await.unwrap();
        let mut name = vec![0u8; n[0] as usize + 2];
        server.read_exact(&mut name).await.unwrap();
        assert_eq!(&name[..n[0] as usize], b"example.com");
        server
            .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
            .await
            .unwrap();
    });

    let target = SocksTarget::Name {
        name: "example.com".into(),
        port: 443,
    };
    socks_connect(
        &mut client,
        5,
        "alice",
        "secret",
        &target,
        SocksCommand::Connect,
    )
    .await
    .unwrap();
    server_task.await.unwrap();
}

#[tokio::test]
async fn socks5_rejected() {
    let (mut client, mut server) = duplex(4096);
    let server_task = tokio::spawn(async move {
        let mut buf = [0u8; 3];
        server.read_exact(&mut buf).await.unwrap();
        server.write_all(&[5, 0]).await.unwrap();
        let mut req = [0u8; 10];
        server.read_exact(&mut req).await.unwrap();
        // reply: connection refused (5)
        server
            .write_all(&[5, 5, 0, 1, 0, 0, 0, 0, 0, 0])
            .await
            .unwrap();
    });

    let target = SocksTarget::Addr(SocketAddr::from(([10, 0, 0, 1], 80)));
    let err = socks_connect(&mut client, 5, "", "", &target, SocksCommand::Connect)
        .await
        .unwrap_err();
    assert!(matches!(err, SocksError::ProxyFailure(_)));
    server_task.await.unwrap();
}

#[tokio::test]
async fn socks4_connect() {
    let (mut client, mut server) = duplex(4096);
    let server_task = tokio::spawn(async move {
        // [4, 1, port, ip, user..., 0]
        let mut buf = [0u8; 9];
        server.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf[0], 4); // SOCKS4
        assert_eq!(buf[1], 1); // CONNECT
        assert_eq!(&buf[2..4], &6881u16.to_be_bytes());
        assert_eq!(&buf[4..8], &[0, 0, 0, 1]);
        assert_eq!(buf[8], 0); // empty userid terminator
        // granted
        server.write_all(&[0, 90, 0, 0, 0, 0, 0, 0]).await.unwrap();
    });

    let target = SocksTarget::Addr(SocketAddr::from(([0, 0, 0, 1], 6881)));
    socks_connect(&mut client, 4, "", "", &target, SocksCommand::Connect)
        .await
        .unwrap();
    server_task.await.unwrap();
}

// ---------- http_stream (HTTP CONNECT) ----------

use bitdaemon_net::http_stream::{base64_encode, http_connect};

#[test]
fn base64_vectors() {
    assert_eq!(base64_encode(b""), "");
    assert_eq!(base64_encode(b"f"), "Zg==");
    assert_eq!(base64_encode(b"fo"), "Zm8=");
    assert_eq!(base64_encode(b"foo"), "Zm9v");
    assert_eq!(base64_encode(b"alice:secret"), "YWxpY2U6c2VjcmV0");
}

#[tokio::test]
async fn http_connect_ok() {
    let (mut client, mut server) = duplex(4096);
    let server_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 1024];
        let n = server.read(&mut buf).await.unwrap();
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(req.starts_with("CONNECT example.com:443 HTTP/1.0\r\n"));
        assert!(req.contains("Host: example.com:443\r\n"));
        assert!(req.contains("Proxy-Authorization: Basic YWxpY2U6c2VjcmV0\r\n"));
        assert!(req.ends_with("\r\n\r\n"));
        server.write_all(b"HTTP/1.0 200 OK\r\n\r\n").await.unwrap();
    });
    http_connect(
        &mut client,
        SocketAddr::from(([127, 0, 0, 1], 443)),
        Some("example.com"),
        "alice",
        "secret",
    )
    .await
    .unwrap();
    server_task.await.unwrap();
}

#[tokio::test]
async fn http_connect_rejected() {
    let (mut client, mut server) = duplex(4096);
    let server_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 1024];
        let _ = server.read(&mut buf).await.unwrap();
        server
            .write_all(b"HTTP/1.0 407 Proxy Auth Required\r\n\r\n")
            .await
            .unwrap();
    });
    let err = http_connect(
        &mut client,
        SocketAddr::from(([127, 0, 0, 1], 443)),
        None,
        "",
        "",
    )
    .await
    .unwrap_err();
    assert!(matches!(
        err,
        bitdaemon_net::http_stream::HttpConnectError::Rejected
    ));
    server_task.await.unwrap();
}

// ---------- bep10 (extended handshake) ----------

use bitdaemon_net::bep10::ExtendedHandshake;
use std::net::{IpAddr, Ipv4Addr};

#[test]
fn bep10_roundtrip() {
    let mut hs = ExtendedHandshake {
        listen_port: Some(6881),
        client_version: Some(b"BitDaemon 0.1".to_vec()),
        your_ip: Some(vec![192, 168, 0, 5]),
        reqq: 250,
        complete_ago: 42,
        upload_only: true,
        ..Default::default()
    };
    hs.messages.insert("ut_metadata".into(), 1);
    hs.messages.insert("ut_pex".into(), 2);
    hs.messages.insert("upload_only".into(), 3);
    hs.messages.insert("ut_holepunch".into(), 4);
    hs.messages.insert("lt_donthave".into(), 5);

    let wire = hs.encode();
    // framing: len u32 = body+2, 0x14, 0x00
    let len = u32::from_be_bytes(wire[..4].try_into().unwrap()) as usize;
    assert_eq!(len + 4, wire.len());
    assert_eq!(wire[4], 0x14);
    assert_eq!(wire[5], 0);

    let parsed = ExtendedHandshake::parse(&wire[6..]).unwrap();
    assert_eq!(parsed.listen_port, Some(6881));
    assert_eq!(
        parsed.client_version.as_deref(),
        Some(&b"BitDaemon 0.1"[..])
    );
    assert_eq!(parsed.reqq, 250);
    assert_eq!(parsed.complete_ago, 42);
    assert!(parsed.upload_only);
    assert!(!parsed.share_mode);
    assert_eq!(parsed.messages.get("ut_metadata"), Some(&1));
    assert_eq!(parsed.messages.get("lt_donthave"), Some(&5));
    assert_eq!(
        parsed.remote_ip(),
        Some(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 5)))
    );

    let ids = parsed.remote_ids();
    assert_eq!(ids.upload_only, 3);
    assert_eq!(ids.holepunch, 4);
    assert_eq!(ids.dont_have, 5);
}

#[test]
fn bep10_parse_garbage() {
    assert!(ExtendedHandshake::parse(b"not bencode").is_none());
    assert!(ExtendedHandshake::parse(b"i42e").is_none()); // not a dict
}

// ---------- udp_tracker (BEP-15) ----------

use bitdaemon_net::transport::DatagramSocket;
use bitdaemon_net::udp_tracker::*;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Debug, Default)]
struct FakeTracker {
    sent: Mutex<Vec<(Vec<u8>, SocketAddr)>>,
    replies: Mutex<Vec<Vec<u8>>>,
}

impl DatagramSocket for FakeTracker {
    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> bitdaemon_net::transport::BoxFut<'a, usize> {
        self.sent.lock().unwrap().push((buf.to_vec(), target));
        Box::pin(async move { Ok(buf.len()) })
    }
    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> bitdaemon_net::transport::BoxFut<'a, (usize, SocketAddr)> {
        Box::pin(async move {
            let r = self.replies.lock().unwrap().remove(0);
            let n = r.len().min(buf.len());
            buf[..n].copy_from_slice(&r[..n]);
            Ok((n, SocketAddr::from(([127, 0, 0, 1], 9999))))
        })
    }
    fn local_addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 0))
    }
}

fn announce_req() -> AnnounceRequest {
    AnnounceRequest {
        info_hash: [0xaa; 20],
        peer_id: [0xbb; 20],
        downloaded: 0,
        left: 1_000_000,
        uploaded: 0,
        event: 2, // started
        announce_ip: Ipv4Addr::UNSPECIFIED,
        key: 0x1234,
        num_want: 50,
        listen_port: 6881,
        request_string: String::new(),
    }
}

#[test]
fn udp_wire_build() {
    let c = build_connect(0xdeadbeef);
    assert_eq!(&c[..8], &0x417_2710_1980u64.to_be_bytes());
    assert_eq!(&c[8..12], &0i32.to_be_bytes());
    assert_eq!(&c[12..16], &0xdeadbeefu32.to_be_bytes());

    let a = build_announce(0x1122_3344_5566_7788, 0x99, &announce_req());
    assert_eq!(a.len(), 98);
    assert_eq!(&a[..8], &0x1122_3344_5566_7788u64.to_be_bytes());
    assert_eq!(&a[8..12], &1i32.to_be_bytes()); // action announce
    assert_eq!(&a[12..16], &0x99u32.to_be_bytes());
    assert_eq!(&a[16..36], &[0xaa; 20]); // info_hash
    assert_eq!(&a[36..56], &[0xbb; 20]); // peer_id
    assert_eq!(&a[96..98], &6881u16.to_be_bytes());
}

#[tokio::test]
async fn udp_announce_flow() {
    let tracker = FakeTracker::default();
    // reply to connect: [0][any tid][conn_id] — tid is random so we
    // echo back the one in the request: emulate by replying after send
    // (fake inspects the sent packet)
    // simpler: pre-program replies — connect reply uses tid 0x0f0f0f0f? we can't
    // know the random tid. Use a scripted responder instead:
    let t = tracker.local_addr();
    let req = announce_req();

    // scripted server that reads the incoming packet tid
    #[derive(Debug)]
    struct Echo {
        local: SocketAddr,
        sent: Mutex<Vec<Vec<u8>>>,
        inner: Mutex<Vec<Vec<u8>>>,
    }
    impl DatagramSocket for Echo {
        fn send_to<'a>(
            &'a self,
            buf: &'a [u8],
            _t: SocketAddr,
        ) -> bitdaemon_net::transport::BoxFut<'a, usize> {
            let tid = u32::from_be_bytes(buf[12..16].try_into().unwrap());
            let reply = if i32::from_be_bytes(buf[8..12].try_into().unwrap()) == ACTION_CONNECT {
                // connect response: [0][tid][conn_id]
                let mut r = Vec::with_capacity(16);
                r.extend(ACTION_CONNECT.to_be_bytes());
                r.extend(tid.to_be_bytes());
                r.extend(0x7777_8888u64.to_be_bytes());
                r
            } else {
                // announce response: [1][tid][interval][incomplete][complete][peers]
                let mut r = Vec::new();
                r.extend(ACTION_ANNOUNCE.to_be_bytes());
                r.extend(tid.to_be_bytes());
                r.extend(1800i32.to_be_bytes());
                r.extend(7i32.to_be_bytes()); // leechers
                r.extend(3i32.to_be_bytes()); // seeders
                r.extend([10, 0, 0, 5]); // peer ip
                r.extend(51413u16.to_be_bytes());
                r
            };
            self.inner.lock().unwrap().push(reply);
            self.sent.lock().unwrap().push(buf.to_vec());
            Box::pin(async move { Ok(buf.len()) })
        }
        fn recv_from<'a>(
            &'a self,
            buf: &'a mut [u8],
        ) -> bitdaemon_net::transport::BoxFut<'a, (usize, SocketAddr)> {
            Box::pin(async move {
                let r = self.inner.lock().unwrap().remove(0);
                buf[..r.len()].copy_from_slice(&r);
                Ok((r.len(), SocketAddr::from(([127, 0, 0, 1], 9999))))
            })
        }
        fn local_addr(&self) -> SocketAddr {
            self.local
        }
    }
    let echo = Echo {
        local: t,
        sent: Mutex::new(vec![]),
        inner: Mutex::new(vec![]),
    };
    let resp = udp_announce(&echo, t, &req, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(resp.interval, 1800);
    assert_eq!(resp.complete, 3);
    assert_eq!(resp.incomplete, 7);
    assert_eq!(resp.peers, vec![SocketAddr::from(([10, 0, 0, 5], 51413))]);
    // two datagrams sent: connect + announce
    assert_eq!(echo.sent.lock().unwrap().len(), 2);
    assert_eq!(
        echo.sent.lock().unwrap()[1][..8].to_vec(),
        0x7777_8888u64.to_be_bytes()
    );
}

// ---------- http_tracker (announce/response) ----------

use bitdaemon_net::http_tracker::*;

#[test]
fn announce_query_format() {
    let q = announce_query(
        &[0xaa; 20],
        &[0xbb; 20],
        6881,
        0,
        500,
        1_000_000,
        2, // started
        50,
        0x1234,
        true,
        true,
        Some(b"tid42"),
    );
    assert!(q.contains("port=6881"));
    assert!(q.contains("downloaded=500"));
    assert!(q.contains("left=1000000"));
    assert!(q.contains("event=started"));
    assert!(q.contains("numwant=50"));
    assert!(q.contains("key=1234"));
    assert!(q.contains("compact=1"));
    assert!(q.contains("no_peer_id=1"));
    assert!(q.contains("trackerid=tid42"));
    assert!(q.contains("info_hash=%aa%aa%aa"));
}

fn dict_resp(body: &[u8]) -> TrackerResponse {
    parse_response(body, false, None).unwrap()
}

#[test]
fn response_compact() {
    // d8:intervali1800e8:completei3e10:incompletei7e5:peers6:...
    let mut body = b"d8:intervali1800e8:completei3e10:incompletei7e5:peers6:".to_vec();
    body.extend([10, 0, 0, 5]);
    body.extend(51413u16.to_be_bytes());
    body.push(b'e');
    let r = dict_resp(&body);
    assert_eq!(r.interval, 1800);
    assert_eq!(r.complete, 3);
    assert_eq!(r.incomplete, 7);
    assert_eq!(r.peers, vec![SocketAddr::from(([10, 0, 0, 5], 51413))]);
}

#[test]
fn response_dict_peers() {
    let body =
        b"d8:intervali60e5:peersld2:ip9:127.0.0.17:peer id20:ABCDEFGHIJKLMNOPQRST4:porti6881eeee";
    let r = dict_resp(body);
    assert_eq!(r.peers, vec![SocketAddr::from(([127, 0, 0, 1], 6881))]);
    assert_eq!(r.peer_ids[0].as_deref(), Some(&b"ABCDEFGHIJKLMNOPQRST"[..]));
}

#[test]
fn response_failure() {
    let body = b"d14:failure reason23:not authorized to announcee";
    let err = parse_response(body, false, None).unwrap_err();
    assert!(matches!(err, HttpTrackerError::Failure(_)));
}

#[test]
fn response_scrape() {
    let mut body = b"d5:filesd20:".to_vec();
    body.extend([0xaa; 20]);
    body.extend(b"d8:completei9e10:downloadedi4e10:incompletei2eeee");
    let r = parse_response(&body, true, Some(&[0xaa; 20])).unwrap();
    assert_eq!(r.complete, 9);
    assert_eq!(r.downloaded, 4);
    assert_eq!(r.incomplete, 2);
}

#[test]
fn response_v6() {
    let mut body = b"d6:peers618:".to_vec();
    body.extend([0x20, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    body.extend(6881u16.to_be_bytes());
    body.push(b'e');
    let r = dict_resp(&body);
    assert_eq!(r.peers.len(), 1);
    assert!(r.peers[0].is_ipv6());
}

// ---------- lsd (BEP-14) ----------

use bitdaemon_net::lsd::*;

#[test]
fn lsd_render() {
    // verbatim format from upstream render_lsd_packet
    let pkt = render_lsd_packet(
        6881,
        "0123456789abcdef0123456789abcdef01234567",
        0x42,
        false,
    );
    let s = String::from_utf8(pkt).unwrap();
    assert_eq!(
        s,
        "BT-SEARCH * HTTP/1.1\r\nHost: 239.192.152.143:6771\r\nPort: 6881\r\nInfohash: 0123456789abcdef0123456789abcdef01234567\r\ncookie: 42\r\n\r\n\r\n"
    );
}

#[test]
fn lsd_parse() {
    let pkt = render_lsd_packet(51413, &"ab".repeat(20), 0x99, false);
    let from = std::net::SocketAddr::from(([192, 168, 1, 5], 6771));
    // cookie differs → accepted
    let a = process_packet(&pkt, from, 0x11).unwrap();
    assert_eq!(a.port, 51413);
    assert_eq!(a.info_hashes[0][0], 0xab);
    // our own cookie → ignored
    assert!(process_packet(&pkt, from, 0x99).is_none());
    // not BT-SEARCH → ignored
    assert!(process_packet(b"GET / HTTP/1.1\r\n\r\n", from, 0x99).is_none());
}
