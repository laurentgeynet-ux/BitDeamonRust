// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `web_seed` request building + `natpmp` codec round-trips.

use bitdaemon_net::natpmp::{NatPmpResponse, PortmapProtocol, build_map_request, parse_response};
use bitdaemon_net::web_seed::{FileRequest, consume_body};

#[test]
fn natpmp_map_request_bytes() {
    let r = build_map_request(PortmapProtocol::Tcp, 6881, 6881, 3600);
    assert_eq!(
        &r[..],
        &[0, 2, 0, 0, 0x1A, 0xE1, 0x1A, 0xE1, 0, 0, 0x0E, 0x10]
    );
}

#[test]
fn natpmp_parse_map_reply() {
    let mut r = build_map_request(PortmapProtocol::Udp, 1111, 2222, 100);
    r[1] = 0x81; // reply bit (op|0x80)
    // response layout differs — craft proper reply
    let reply = [0, 1, 0, 0, 0, 0, 0, 0, 4, 87, 8, 174, 0, 0, 0, 100];
    let _ = r;
    match parse_response(&reply) {
        Some(NatPmpResponse::Map {
            private_port,
            public_port,
            lifetime,
            result,
            ..
        }) => {
            assert_eq!(private_port, 1111);
            assert_eq!(public_port, 2222);
            assert_eq!(lifetime, 100);
            assert_eq!(result, 0);
        }
        _ => panic!("expected map reply"),
    }
}

#[test]
fn natpmp_parse_public_ip() {
    // upstream requires cmd == 0x80 for a public-address reply
    let reply = [0, 0x80, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8];
    assert_eq!(
        parse_response(&reply),
        Some(NatPmpResponse::PublicIp("8.8.8.8".parse().unwrap()))
    );
}

#[test]
fn web_seed_consume_body_splits_files() {
    let files = [
        FileRequest {
            file_index: 0,
            start: 0,
            length: 10,
            got_get: true,
        },
        FileRequest {
            file_index: 1,
            start: 0,
            length: 10,
            got_get: true,
        },
    ];
    let parts = consume_body(&files, 15);
    assert_eq!(parts, vec![(0, 0, 10), (1, 0, 5)]);
}

#[tokio::test]
async fn fetch_block_over_duplex() {
    use bitdaemon_net::transport::BoxedPeerStream;
    use bitdaemon_net::web_seed::{FileRequest, WebSeedRequest, fetch_block};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (a, b) = tokio::io::duplex(64 * 1024);
    let mut cli: BoxedPeerStream = Box::new(a);
    let mut srv: BoxedPeerStream = Box::new(b);

    tokio::spawn(async move {
        let mut buf = [0u8; 2048];
        let n = srv.read(&mut buf).await.unwrap();
        let req = String::from_utf8_lossy(&buf[..n]);
        assert!(req.contains("Range: bytes=16-31"));
        let body = [0xABu8; 16];
        let head = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\n\
             Content-Range: bytes 16-31/64\r\n\r\n",
            body.len()
        );
        srv.write_all(head.as_bytes()).await.unwrap();
        srv.write_all(&body).await.unwrap();
    });

    let req = WebSeedRequest {
        base: "http://s.example".to_string(),
        request: "GET /file.bin HTTP/1.1\r\nHost: s.example\r\nRange: bytes=16-31\r\n\r\n"
            .to_string(),
        files: vec![FileRequest {
            file_index: 0,
            start: 16,
            length: 16,
            got_get: true,
        }],
    };
    let (segs, _close) = fetch_block(&mut cli, &req, 64 * 1024).await.unwrap();
    assert_eq!(segs.len(), 1);
    assert_eq!((segs[0].0, segs[0].1), (0, 16));
    assert_eq!(segs[0].2, vec![0xAB; 16]);
}

/// 206 without `Content-Range` → `errors::invalid_range` → the seed
/// is `disable()`d upstream; `WebSeedError::InvalidRange` here.
#[tokio::test]
async fn fetch_block_rejects_206_without_content_range() {
    use bitdaemon_net::transport::BoxedPeerStream;
    use bitdaemon_net::web_seed::{FileRequest, WebSeedError, WebSeedRequest, fetch_block};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (a, b) = tokio::io::duplex(64 * 1024);
    let mut cli: BoxedPeerStream = Box::new(a);
    let mut srv: BoxedPeerStream = Box::new(b);

    tokio::spawn(async move {
        let mut buf = [0u8; 2048];
        let _ = srv.read(&mut buf).await.unwrap();
        srv.write_all(b"HTTP/1.1 206 Partial Content\r\nContent-Length: 16\r\n\r\n")
            .await
            .unwrap();
        srv.write_all(&[0xABu8; 16]).await.unwrap();
    });

    let req = WebSeedRequest {
        base: "http://s.example".to_string(),
        request: "GET /file.bin HTTP/1.1\r\nHost: s.example\r\nRange: bytes=16-31\r\n\r\n"
            .to_string(),
        files: vec![FileRequest {
            file_index: 0,
            start: 16,
            length: 16,
            got_get: true,
        }],
    };
    let err = fetch_block(&mut cli, &req, 64 * 1024).await.unwrap_err();
    assert!(matches!(err, WebSeedError::InvalidRange));
}

/// 206 answering a *different* range → upstream `invalid_range` via
/// `disconnect` (retryable, not a disable) → `RangeMismatch`.
#[tokio::test]
async fn fetch_block_rejects_mismatched_content_range() {
    use bitdaemon_net::transport::BoxedPeerStream;
    use bitdaemon_net::web_seed::{FileRequest, WebSeedError, WebSeedRequest, fetch_block};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (a, b) = tokio::io::duplex(64 * 1024);
    let mut cli: BoxedPeerStream = Box::new(a);
    let mut srv: BoxedPeerStream = Box::new(b);

    tokio::spawn(async move {
        let mut buf = [0u8; 2048];
        let _ = srv.read(&mut buf).await.unwrap();
        // well-formed range, wrong offset — the byte-stream would
        // otherwise land at the wrong file position and hash-fail.
        srv.write_all(
            b"HTTP/1.1 206 Partial Content\r\nContent-Length: 16\r\n\
              Content-Range: bytes 0-15/64\r\n\r\n",
        )
        .await
        .unwrap();
        srv.write_all(&[0xABu8; 16]).await.unwrap();
    });

    let req = WebSeedRequest {
        base: "http://s.example".to_string(),
        request: "GET /file.bin HTTP/1.1\r\nHost: s.example\r\nRange: bytes=16-31\r\n\r\n"
            .to_string(),
        files: vec![FileRequest {
            file_index: 0,
            start: 16,
            length: 16,
            got_get: true,
        }],
    };
    let err = fetch_block(&mut cli, &req, 64 * 1024).await.unwrap_err();
    assert!(matches!(
        err,
        WebSeedError::RangeMismatch {
            start: 0,
            end: 16,
            ..
        }
    ));
}

/// non-206 without `Content-Length` → `errors::no_content_length`
/// → `disable()` upstream; `InvalidRange` here.
#[tokio::test]
async fn fetch_block_rejects_200_without_content_length() {
    use bitdaemon_net::transport::BoxedPeerStream;
    use bitdaemon_net::web_seed::{FileRequest, WebSeedError, WebSeedRequest, fetch_block};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (a, b) = tokio::io::duplex(64 * 1024);
    let mut cli: BoxedPeerStream = Box::new(a);
    let mut srv: BoxedPeerStream = Box::new(b);

    tokio::spawn(async move {
        let mut buf = [0u8; 2048];
        let _ = srv.read(&mut buf).await.unwrap();
        // chunked transfer with no Content-Length header at all.
        srv.write_all(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
              10\r\n0123456789abcdef\r\n0\r\n\r\n",
        )
        .await
        .unwrap();
    });

    let req = WebSeedRequest {
        base: "http://s.example".to_string(),
        request: "GET /file.bin HTTP/1.1\r\nHost: s.example\r\nRange: bytes=0-15\r\n\r\n"
            .to_string(),
        files: vec![FileRequest {
            file_index: 0,
            start: 0,
            length: 16,
            got_get: true,
        }],
    };
    let err = fetch_block(&mut cli, &req, 64 * 1024).await.unwrap_err();
    assert!(matches!(err, WebSeedError::InvalidRange));
}

/// `Retry-After` on a 503 is parsed and forwarded — upstream feeds
/// it straight into `retry_web_seed`.
#[tokio::test]
async fn fetch_block_surfaces_retry_after() {
    use bitdaemon_net::transport::BoxedPeerStream;
    use bitdaemon_net::web_seed::{FileRequest, WebSeedError, WebSeedRequest, fetch_block};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (a, b) = tokio::io::duplex(64 * 1024);
    let mut cli: BoxedPeerStream = Box::new(a);
    let mut srv: BoxedPeerStream = Box::new(b);

    tokio::spawn(async move {
        let mut buf = [0u8; 2048];
        let _ = srv.read(&mut buf).await.unwrap();
        srv.write_all(
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\
              Retry-After: 90\r\n\r\n",
        )
        .await
        .unwrap();
    });

    let req = WebSeedRequest {
        base: "http://s.example".to_string(),
        request: "GET /file.bin HTTP/1.1\r\nHost: s.example\r\nRange: bytes=0-15\r\n\r\n"
            .to_string(),
        files: vec![FileRequest {
            file_index: 0,
            start: 0,
            length: 16,
            got_get: true,
        }],
    };
    let err = fetch_block(&mut cli, &req, 64 * 1024).await.unwrap_err();
    match err {
        WebSeedError::Http {
            status,
            retry_after,
            ..
        } => {
            assert_eq!(status, 503);
            assert_eq!(retry_after, Some(std::time::Duration::from_secs(90)));
        }
        e => panic!("expected Http error, got {e:?}"),
    }
}

/// a plain `Connection: close` answer flips the keep-alive flag —
/// upstream rebuilds the socket on `m_received_disconnect`.
#[tokio::test]
async fn fetch_block_reports_connection_close() {
    use bitdaemon_net::transport::BoxedPeerStream;
    use bitdaemon_net::web_seed::{FileRequest, WebSeedRequest, fetch_block};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (a, b) = tokio::io::duplex(64 * 1024);
    let mut cli: BoxedPeerStream = Box::new(a);
    let mut srv: BoxedPeerStream = Box::new(b);

    tokio::spawn(async move {
        let mut buf = [0u8; 2048];
        let _ = srv.read(&mut buf).await.unwrap();
        srv.write_all(
            b"HTTP/1.0 206 Partial Content\r\nContent-Length: 16\r\n\
              Content-Range: bytes 16-31/64\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
        srv.write_all(&[0xABu8; 16]).await.unwrap();
    });

    let req = WebSeedRequest {
        base: "http://s.example".to_string(),
        request: "GET /file.bin HTTP/1.1\r\nHost: s.example\r\nRange: bytes=16-31\r\n\r\n"
            .to_string(),
        files: vec![FileRequest {
            file_index: 0,
            start: 16,
            length: 16,
            got_get: true,
        }],
    };
    let (_segs, close) = fetch_block(&mut cli, &req, 64 * 1024).await.unwrap();
    assert!(close, "Connection: close must propagate");
}
