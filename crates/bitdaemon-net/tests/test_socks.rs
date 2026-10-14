// SOCKS4/5 handshake tests — a scripted server over `tokio::io::duplex`
// validates the wire protocol byte-for-byte.

use bitdaemon_net::socks::{SocksCommand, SocksError, SocksTarget, socks_connect};
use std::net::SocketAddr;
use std::sync::Arc;
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
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

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
        3,      // corrupt
        7,      // redundant
        0x1234, // key — rendered %08X upstream
        2,      // started
        50,
        Some(b"tid42"),
        &AnnounceOptions::default(),
        &[Ipv4Addr::new(1, 2, 3, 4)],
        &[Ipv6Addr::LOCALHOST],
    );
    assert!(q.contains("port=6881"));
    assert!(q.contains("downloaded=500"));
    assert!(q.contains("left=1000000"));
    assert!(q.contains("corrupt=3"));
    assert!(q.contains("event=started"));
    assert!(q.contains("numwant=50"));
    // upstream `%08X` — uppercase, zero-padded
    assert!(q.contains("key=00001234"));
    assert!(q.contains("compact=1"));
    assert!(q.contains("no_peer_id=1"));
    // default opts: crypto + redundant reporting on
    assert!(q.contains("supportcrypto=1"));
    assert!(q.contains("redundant=7"));
    assert!(q.contains("trackerid=tid42"));
    assert!(q.contains("ipv4=1.2.3.4"));
    assert!(q.contains("ipv6=%3a%3a1"));
    assert!(q.contains("info_hash=%aa%aa%aa"));
}

fn dict_resp(body: &[u8]) -> TrackerResponse {
    parse_response(body, false, None, false).unwrap()
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
    let err = parse_response(body, false, None, false).unwrap_err();
    assert!(matches!(err, HttpTrackerError::Failure(_)));
}

#[test]
fn response_scrape() {
    let mut body = b"d5:filesd20:".to_vec();
    body.extend([0xaa; 20]);
    body.extend(b"d8:completei9e10:downloadedi4e10:incompletei2eeee");
    let r = parse_response(&body, true, Some(&[0xaa; 20]), false).unwrap();
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

// ---------- socks_udp (SOCKS5 datagram framing) ----------

use bitdaemon_net::socks_udp::*;

#[test]
fn socks5_udp_v4_roundtrip() {
    let ep: std::net::SocketAddr = "1.2.3.4:6881".parse().unwrap();
    let pkt = socks5_wrap(ep, b"payload");
    // [0,0][0][1][1.2.3.4][6881] + payload
    assert_eq!(&pkt[..4], &[0, 0, 0, 1]);
    assert_eq!(&pkt[4..8], &[1, 2, 3, 4]);
    assert_eq!(&pkt[8..10], &6881u16.to_be_bytes());
    let (from, body) = socks5_unwrap(&pkt).unwrap();
    match from {
        Socks5From::Addr(a) => assert_eq!(a, ep),
        _ => panic!(),
    }
    assert_eq!(body, b"payload");
}

#[test]
fn socks5_udp_v6_hostname_frag() {
    let ep: std::net::SocketAddr = "[::1]:443".parse().unwrap();
    let pkt = socks5_wrap(ep, b"x");
    assert_eq!(pkt[3], 4);
    let (from, body) = socks5_unwrap(&pkt).unwrap();
    assert!(matches!(from, Socks5From::Addr(a) if a == ep));
    assert_eq!(body, b"x");

    // hostname
    let pkt = socks5_wrap_hostname("example.com", 80, b"p");
    match socks5_unwrap(&pkt).unwrap().0 {
        Socks5From::Hostname(h) => assert_eq!(h, "example.com"),
        _ => panic!(),
    }

    // FRAG != 0 rejected
    let mut pkt = socks5_wrap(ep, b"x");
    pkt[2] = 1;
    assert!(socks5_unwrap(&pkt).is_none());
    // too short
    assert!(socks5_unwrap(&[0u8; 10]).is_none());
}

// ---------- parse_url ----------

use bitdaemon_net::parse_url::*;

#[test]
fn url_components() {
    let c = parse_url_components("http://user:pw@host.com:8080/p?q=1").unwrap();
    assert_eq!(c.protocol, "http");
    assert_eq!(c.auth, "user:pw");
    assert_eq!(c.hostname, "host.com");
    assert_eq!(c.port, 8080);
    assert_eq!(c.path, "/p?q=1");

    let c = parse_url_components("udp://[fe80::1]:6969/").unwrap();
    assert_eq!(c.hostname, "fe80::1");
    assert_eq!(c.port, 6969);

    // missing path → "/"
    assert_eq!(parse_url_components("http://h").unwrap().path, "/");
    // query without path → "/?…"
    assert_eq!(parse_url_components("http://h?x").unwrap().path, "/?x");
    // invalid
    assert!(parse_url_components("noscheme").is_err());
    assert!(parse_url_components("http://bad host/").is_err());
    assert!(parse_url_components("http://h:xx/").is_err());
    assert!(parse_url_components("http://[fe80::1/").is_err());
}

#[test]
fn url_helpers() {
    assert_eq!(
        split_url("http://h.com/announce").unwrap(),
        ("http://h.com".to_string(), "/announce".to_string())
    );
    assert!(split_url("no-scheme").is_err());

    assert!(is_idna("xn--bcher.example"));
    assert!(!is_idna("example.com"));

    assert!(has_tracker_query_string("info_hash=x&port=1"));
    assert!(!has_tracker_query_string("foo=1&bar=2"));

    assert!(same_origin("http://h.com/a", "HTTP://h.com/b"));
    assert!(same_origin("https://h.com:443/", "https://h.com/x"));
    assert!(!same_origin("http://h.com/", "https://h.com/"));
    assert!(!same_origin("http://a.com/", "http://b.com/"));

    assert!(is_valid_tracker_url("http://t/announce"));
    assert!(is_valid_tracker_url("udp://t:80/"));
    assert!(!is_valid_tracker_url("ftp://t/"));
    assert!(!is_valid_tracker_url("http://t/a b")); // space
}

// ---------- http_parser ----------

use bitdaemon_net::http_parser::HttpParser;

#[test]
fn http_response() {
    let mut p = HttpParser::new(0);
    let mut err = false;
    let buf = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello";
    p.incoming(buf, &mut err);
    assert!(!err);
    assert!(p.header_finished());
    assert_eq!(p.status_code(), 200);
    assert_eq!(p.header("content-length"), Some("5"));
    assert_eq!(p.content_length(), 5);
    assert!(p.connection_close());
    assert_eq!(p.get_body(buf), b"hello");
    assert!(p.finished());
}

#[test]
fn http_request_and_100_continue() {
    let mut p = HttpParser::new(0);
    let mut err = false;
    let buf = b"GET /announce?info_hash=x HTTP/1.1\r\nHost: t\r\n\r\n";
    p.incoming(buf, &mut err);
    assert!(!err);
    assert_eq!(p.method(), "get");
    assert_eq!(p.path(), "/announce?info_hash=x");

    // 100 Continue → headers restart
    let mut p = HttpParser::new(0);
    let buf = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    p.incoming(buf, &mut err);
    assert!(!err);
    assert_eq!(p.status_code(), 200);
}

#[test]
fn http_chunked() {
    let mut p = HttpParser::new(0);
    let mut err = false;
    let buf =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n";
    p.incoming(buf, &mut err);
    assert!(!err);
    assert!(p.chunked_encoding());
    assert!(p.finished());
    assert_eq!(p.chunks().len(), 2);
    // reassemble body from chunk ranges
    let body: Vec<u8> = p
        .chunks()
        .iter()
        .flat_map(|&(s, e)| buf[s as usize..e as usize].iter().copied())
        .collect();
    assert_eq!(body, b"abcde");
}

#[test]
fn http_content_range() {
    let mut p = HttpParser::new(0);
    let mut err = false;
    let buf = b"HTTP/1.1 206 Partial\r\nContent-Range: bytes 10-19/100\r\n\r\n0123456789";
    p.incoming(buf, &mut err);
    assert!(!err);
    assert_eq!(p.content_range(), (10, 19));
    assert_eq!(p.content_length(), 10);
}

// ---------- ip_voter ----------

use bitdaemon_net::ip_voter::{IpSourceFlags, IpVoter};

#[test]
fn ip_voter_majority() {
    let mut v = IpVoter::new();
    let ext: std::net::IpAddr = "85.1.2.3".parse().unwrap();
    let s1: std::net::IpAddr = "9.9.9.1".parse().unwrap();
    let s2: std::net::IpAddr = "9.9.9.2".parse().unwrap();
    let s3: std::net::IpAddr = "9.9.9.3".parse().unwrap();
    // local/loopback/unspecified rejected
    assert!(!v.cast_vote("10.0.0.1".parse().unwrap(), IpSourceFlags::SOURCE_PEER, &s1));
    assert!(!v.cast_vote(
        "127.0.0.1".parse().unwrap(),
        IpSourceFlags::SOURCE_PEER,
        &s1
    ));
    // first vote installs it
    assert!(v.cast_vote(ext, IpSourceFlags::SOURCE_PEER, &s1));
    assert_eq!(v.external_address(), Some(ext));
    // same voter can't double-vote
    assert!(!v.cast_vote(ext, IpSourceFlags::SOURCE_PEER, &s1));
    // af mismatch rejected
    assert!(!v.cast_vote("2001::1".parse().unwrap(), IpSourceFlags::SOURCE_PEER, &s1));
    // new distinct voters change nothing while valid
    v.cast_vote(ext, IpSourceFlags::SOURCE_TRACKER, &s2);
    v.cast_vote(ext, IpSourceFlags::SOURCE_DHT, &s3);
    assert_eq!(v.external_address(), Some(ext));
}

/// UDP-ASSOCIATE end-to-end: scripted SOCKS5 server hands out a relay
/// BND address; outbound datagrams arrive wrapped at the relay,
/// inbound wrapped datagrams unwrap to the original source.
#[tokio::test]
async fn socks5_udp_associate_relay() {
    use bitdaemon_net::socks_udp::{Socks5UdpSocket, socks5_unwrap, socks5_wrap};
    use bitdaemon_net::transport::{DatagramSocket, PeerConnector};
    use std::fmt::Debug;
    use tokio::sync::mpsc;

    // mock PeerConnector — always yields one end of a duplex pair;
    // the stream crosses a oneshot because `PeerConnector::connect`
    // requires a `Sync` future while `BoxedPeerStream` is not `Sync`.
    #[derive(Debug)]
    struct C {
        peer: std::sync::Mutex<Option<BoxedPeerStream>>,
    }
    use bitdaemon_net::transport::BoxedPeerStream;
    impl PeerConnector for C {
        fn connect(
            self: Arc<Self>,
            _addr: SocketAddr,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = bitdaemon_net::Result<BoxedPeerStream>>
                    + Send
                    + Sync,
            >,
        > {
            let s = self.peer.lock().unwrap().take();
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(s);
            Box::pin(async move {
                rx.await
                    .map_err(|_| bitdaemon_net::NetError::NoTransport("no mock"))?
                    .ok_or(bitdaemon_net::NetError::NoTransport("no mock"))
            })
        }
    }

    // channel "UDP socket" — captures what we send to the relay and
    // feeds us scripted inbound datagrams
    let (in_tx, in_rx) = mpsc::unbounded_channel::<(Vec<u8>, SocketAddr)>();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<(Vec<u8>, SocketAddr)>();
    struct U {
        rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<(Vec<u8>, SocketAddr)>>,
        tx: mpsc::UnboundedSender<(Vec<u8>, SocketAddr)>,
        me: SocketAddr,
    }
    impl Debug for U {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("U").finish()
        }
    }
    impl DatagramSocket for U {
        fn send_to<'a>(
            &'a self,
            buf: &'a [u8],
            to: SocketAddr,
        ) -> bitdaemon_net::transport::BoxFut<'a, usize> {
            Box::pin(async move {
                let _ = self.tx.send((buf.to_vec(), to));
                Ok(buf.len())
            })
        }
        fn recv_from<'a>(
            &'a self,
            buf: &'a mut [u8],
        ) -> bitdaemon_net::transport::BoxFut<'a, (usize, SocketAddr)> {
            Box::pin(async move {
                let (d, from) = self
                    .rx
                    .lock()
                    .await
                    .recv()
                    .await
                    .ok_or_else(|| std::io::Error::other("closed"))?;
                let n = d.len().min(buf.len());
                buf[..n].copy_from_slice(&d[..n]);
                Ok((n, from))
            })
        }
        fn local_addr(&self) -> SocketAddr {
            self.me
        }
    }

    let (client, mut server) = duplex(4096);
    let relay = SocketAddr::from(([127, 0, 0, 1], 19000));
    let server_task = tokio::spawn(async move {
        // method negotiation
        let mut m = [0u8; 3];
        server.read_exact(&mut m).await.unwrap();
        assert_eq!(m[0], 5);
        server.write_all(&[5, 0]).await.unwrap();
        // request: UDP ASSOCIATE (cmd=3), DST 0.0.0.0:0
        let mut req = [0u8; 10];
        server.read_exact(&mut req).await.unwrap();
        assert_eq!(&req[..4], &[5, 3, 0, 1]);
        assert_eq!(&req[4..], &[0, 0, 0, 0, 0, 0]);
        // reply: success, BND = relay
        let mut rep = vec![5u8, 0, 0, 1];
        rep.extend_from_slice(&[127, 0, 0, 1]);
        rep.extend_from_slice(&19000u16.to_be_bytes());
        server.write_all(&rep).await.unwrap();
        // keep the control connection open — closing ends the
        // association per RFC 1928
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    });

    let proxy = SocketAddr::from(([127, 0, 0, 1], 1080));
    let udp = Arc::new(U {
        rx: tokio::sync::Mutex::new(in_rx),
        tx: out_tx,
        me: SocketAddr::from(([10, 0, 0, 9], 5555)),
    });
    let sock = Socks5UdpSocket::associate(
        Arc::new(C {
            peer: std::sync::Mutex::new(Some(Box::new(client) as BoxedPeerStream)),
        }),
        udp,
        proxy,
        "",
        "",
    )
    .await
    .expect("associate");
    assert_eq!(sock.local_addr(), relay);

    // outbound: wrapped datagram lands at the relay address
    let target = SocketAddr::from(([192, 168, 5, 7], 51413));
    sock.send_to(b"dht-ping", target).await.unwrap();
    let (dgram, to) = out_rx.recv().await.unwrap();
    assert_eq!(to, relay);
    assert_eq!(dgram, socks5_wrap(target, b"dht-ping"));

    // inbound: a relayed datagram unwraps to its real source
    let src = SocketAddr::from(([203, 0, 113, 9], 6881));
    in_tx.send((socks5_wrap(src, b"dht-pong"), relay)).unwrap();
    let mut buf = [0u8; 128];
    let (n, from) = sock.recv_from(&mut buf).await.unwrap();
    assert_eq!(from, src);
    assert_eq!(&buf[..n], b"dht-pong");

    // unwrap rejects fragmented datagrams like upstream
    let mut frag = socks5_wrap(src, b"x");
    frag[2] = 1;
    assert!(socks5_unwrap(&frag).is_none());

    server_task.await.unwrap();
}
