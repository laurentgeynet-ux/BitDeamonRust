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
