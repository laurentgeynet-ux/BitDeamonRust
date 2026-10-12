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
            protocol: _,
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
    let reply = [0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8];
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
            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        srv.write_all(head.as_bytes()).await.unwrap();
        srv.write_all(&body).await.unwrap();
    });

    let req = WebSeedRequest {
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
