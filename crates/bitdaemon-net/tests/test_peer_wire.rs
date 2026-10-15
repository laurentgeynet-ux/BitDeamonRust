//! Conformance tests for the peer wire codec — handshake layout and
//! per-message byte encodings verified against upstream
//! `bt_peer_connection::write_*` wire format.

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use bitdaemon_common::types::PieceIndex;
use bitdaemon_common::{PeerId, PeerRequest, Sha1Hash, Sha256Hash};
use bitdaemon_net::peer_wire::{
    HANDSHAKE_LEN, Handshake, HashRequestWire, MessageId, PeerMessage, PeerWireCodec, decode,
    encode, parse_handshake, write_handshake,
};

fn req(p: i32, s: i32, l: i32) -> PeerRequest {
    PeerRequest {
        piece: PieceIndex(p),
        start: s,
        length: l,
    }
}

fn hr() -> HashRequestWire {
    HashRequestWire {
        file_root: Sha256Hash::from_bytes(&[7u8; 32]).unwrap_or_default(),
        base: 1,
        index: 2,
        count: 3,
        proof_layers: 4,
    }
}

#[test]
fn handshake_layout() {
    let ih = Sha1Hash::from_bytes(&[0xabu8; 20]).unwrap_or_default();
    let pid = PeerId::from_bytes(&[0x42u8; 20]).unwrap_or_default();
    let h = write_handshake(&ih, &pid, true, true, true, true);

    assert_eq!(h.len(), HANDSHAKE_LEN);
    assert_eq!(h[0], 19);
    assert_eq!(&h[1..20], b"BitTorrent protocol");
    // reserved bits verbatim: DHT r7|=0x01, ext r5|=0x10, fast r7|=0x04, v2 r7|=0x10
    assert_eq!(h[20 + 5] & 0x10, 0x10);
    assert_eq!(h[20 + 7] & 0x01, 0x01);
    assert_eq!(h[20 + 7] & 0x04, 0x04);
    assert_eq!(h[20 + 7] & 0x10, 0x10);
    assert_eq!(&h[28..48], &[0xabu8; 20]);
    assert_eq!(&h[48..68], &[0x42u8; 20]);

    let parsed: Handshake = parse_handshake(&h).expect("parse");
    assert!(parsed.supports_dht());
    assert!(parsed.supports_extension());
    assert!(parsed.supports_fast());
    assert!(parsed.supports_v2());
    assert_eq!(parsed.info_hash, ih);
    assert_eq!(parsed.peer_id, pid);
}

#[test]
fn handshake_rejects_bad_pstr() {
    let mut h = [0u8; HANDSHAKE_LEN];
    h[0] = 19;
    h[1] = b'X';
    assert!(parse_handshake(&h).is_err());
}

#[test]
fn message_encodings_byte_exact() {
    // choke: 00 00 00 01 00
    assert_eq!(encode(&PeerMessage::Choke).as_ref(), &[0, 0, 0, 1, 0]);
    // keepalive
    assert_eq!(encode(&PeerMessage::Keepalive).as_ref(), &[0, 0, 0, 0]);
    // have: len 5, id 4, u32 piece
    let have = encode(&PeerMessage::Have(PieceIndex(42)));
    assert_eq!(have.as_ref(), &[0, 0, 0, 5, 4, 0, 0, 0, 42]);
    // request: len 13, id 6, piece/start/len
    let r = encode(&PeerMessage::Request(req(1, 0x4000, 16384)));
    assert_eq!(
        r.as_ref(),
        &[0, 0, 0, 13, 6, 0, 0, 0, 1, 0, 0, 0x40, 0, 0, 0, 0x40, 0]
    );
    // piece: len 9+n, id 7, index, offset, data
    let p = encode(&PeerMessage::Piece {
        piece: PieceIndex(3),
        offset: 8192,
        data: bytes::Bytes::from_static(&[0xde, 0xad]),
    });
    assert_eq!(
        p.as_ref(),
        &[0, 0, 0, 11, 7, 0, 0, 0, 3, 0, 0, 0x20, 0, 0xde, 0xad]
    );
    // dht_port: len 3, id 9, u16
    assert_eq!(
        encode(&PeerMessage::DhtPort(6881)).as_ref(),
        &[0, 0, 0, 3, 9, 0x1a, 0xe1]
    );
    // suggest: len 5, id 0xd
    assert_eq!(
        encode(&PeerMessage::SuggestPiece(PieceIndex(7))).as_ref(),
        &[0, 0, 0, 5, 0xd, 0, 0, 0, 7]
    );
    // have_all: len 1, id 0xe
    assert_eq!(encode(&PeerMessage::HaveAll).as_ref(), &[0, 0, 0, 1, 0xe]);
    // extended: len 2+n, id 20, ext id, payload
    let e = encode(&PeerMessage::Extended {
        id: 0,
        data: bytes::Bytes::from_static(b"d1:ei1ee"),
    });
    assert_eq!(
        e.as_ref(),
        &[
            0, 0, 0, 10, 20, 0, b'd', b'1', b':', b'e', b'i', b'1', b'e', b'e'
        ]
    );
    // hash_request: len 49, id 21, root(32) + 4×u32
    let hreq = encode(&PeerMessage::HashRequest(hr()));
    assert_eq!(hreq.len(), 4 + 49);
    assert_eq!(hreq[4], 21);
    assert_eq!(&hreq[5..37], &[7u8; 32]);
    assert_eq!(&hreq[37..41], &1u32.to_be_bytes());
}

#[test]
fn round_trip_all() {
    let msgs = vec![
        PeerMessage::Keepalive,
        PeerMessage::Choke,
        PeerMessage::Unchoke,
        PeerMessage::Interested,
        PeerMessage::NotInterested,
        PeerMessage::Have(PieceIndex(17)),
        PeerMessage::Bitfield(bytes::Bytes::from_static(&[0xf0, 0x0f])),
        PeerMessage::Request(req(9, 8192, 16384)),
        PeerMessage::Piece {
            piece: PieceIndex(9),
            offset: 8192,
            data: bytes::Bytes::from(vec![0xabu8; 16384]),
        },
        PeerMessage::Cancel(req(9, 8192, 16384)),
        PeerMessage::DhtPort(6881),
        PeerMessage::SuggestPiece(PieceIndex(2)),
        PeerMessage::HaveAll,
        PeerMessage::HaveNone,
        PeerMessage::RejectRequest(req(1, 2, 3)),
        PeerMessage::AllowedFast(PieceIndex(4)),
        PeerMessage::Extended {
            id: 3,
            data: bytes::Bytes::from_static(b"li42e"),
        },
        PeerMessage::HashRequest(hr()),
        // upstream `on_hashes` requires exactly `count +
        // max(0, proof_layers - (merkle_num_layers(leafs(count))-1))`
        // hashes: count=3 → leafs 4 → 2 layers → base depth 1;
        // proof_layers=4 → 3 proofs → 3+3 = 6 hashes.
        PeerMessage::Hashes {
            req: hr(),
            hashes: (1u8..=6)
                .map(|b| Sha256Hash::from_bytes(&[b; 32]).unwrap_or_default())
                .collect(),
        },
        PeerMessage::HashReject(hr()),
    ];

    let mut codec = PeerWireCodec::default();
    for m in msgs {
        let mut buf = BytesMut::new();
        codec.encode(m.clone(), &mut buf).expect("encode");
        let got = codec.decode(&mut buf).expect("decode").expect("frame");
        assert_eq!(got, m);
        assert!(buf.is_empty(), "no leftover bytes for {m:?}");
    }
}

#[test]
fn decoder_waits_for_full_frame() {
    let mut codec = PeerWireCodec::default();
    let mut buf = BytesMut::new();
    codec
        .encode(PeerMessage::Have(PieceIndex(5)), &mut buf)
        .expect("encode");
    // feed only the length prefix + partial payload
    let mut partial = buf.split_to(6);
    assert!(codec.decode(&mut partial).expect("dec").is_none());
    // feed the rest
    partial.extend_from_slice(&buf);
    let got = codec.decode(&mut partial).expect("dec").expect("frame");
    assert_eq!(got, PeerMessage::Have(PieceIndex(5)));
}

#[test]
fn decoder_enforces_max_len() {
    let mut codec = PeerWireCodec::new(16);
    let mut buf = BytesMut::from(&[0, 0, 0, 20, 4][..]);
    assert!(codec.decode(&mut buf).is_err());
}

#[test]
fn decoder_default_cap_is_one_mib() {
    // upstream `bt_peer_connection` kills the conn when the length
    // prefix announces `packet_size > 1024 * 1024`.
    let mut codec = PeerWireCodec::default();
    // exactly 1 MiB body — legal, waits for the frame bytes.
    let mut ok = BytesMut::from(&[0x00, 0x10, 0x00, 0x00][..]);
    assert!(codec.decode(&mut ok).expect("dec").is_none());
    assert_eq!(ok.len(), 4, "prefix stays buffered");
    // 1 MiB + 1 — `packet_too_large`.
    let mut over = BytesMut::from(&[0x00, 0x10, 0x00, 0x01][..]);
    assert!(codec.decode(&mut over).is_err());
}

#[test]
fn short_piece_decodes_to_silent_malformed() {
    // upstream `on_piece` consumes a frame shorter than the 9-byte
    // header silently — the conn lives and the packet still counts
    // toward `num_incoming_piece` (the session's counter maps the
    // stored wire id).
    let msg = decode(&[MessageId::Piece as u8, 0, 0, 0, 1]).expect("dec");
    assert_eq!(msg, PeerMessage::Malformed { id: 7 });
    // exactly 9 bytes parses as a real (empty) piece message — the
    // empty-piece BitComet quirk is handled by the session, not here.
    let full = decode(&[MessageId::Piece as u8, 0, 0, 0, 1, 0, 0, 0, 0]).expect("dec");
    assert!(matches!(full, PeerMessage::Piece { .. }));
}

#[test]
fn decode_unit() {
    // upstream packet_type dispatch on the raw payload
    assert_eq!(
        decode(&[MessageId::Choke as u8]).expect("d"),
        PeerMessage::Choke
    );
    assert_eq!(decode(&[]).expect("d"), PeerMessage::Keepalive);
    assert!(decode(&[0x55]).is_err(), "unknown id");
    // unknown wire id — upstream's `default` leg is an
    // `invalid_message` disconnect *before* the counter block.
    match decode(&[0x55]) {
        Err(bitdaemon_net::peer_wire::WireError::UnknownId(0x55)) => {}
        other => panic!("expected UnknownId(0x55), got {other:?}"),
    }
}

#[test]
fn socket_io_endpoint_codec() {
    use bitdaemon_net::socket_io::{Cursor, endpoint_to_bytes, read_endpoint};
    use std::net::SocketAddr;

    let v4: SocketAddr = "192.168.1.2:6881"
        .parse()
        .unwrap_or_else(|_| unreachable!());
    let b4 = endpoint_to_bytes(v4);
    assert_eq!(b4.len(), 6);
    assert_eq!(b4, [192, 168, 1, 2, 0x1a, 0xe1]);
    let mut c = Cursor::new(&b4);
    assert_eq!(read_endpoint(&mut c, false).expect("v4"), v4);

    let v6: SocketAddr = "[2001:db8::1]:443"
        .parse()
        .unwrap_or_else(|_| unreachable!());
    let b6 = endpoint_to_bytes(v6);
    assert_eq!(b6.len(), 18);
    let mut c6 = Cursor::new(&b6);
    assert_eq!(read_endpoint(&mut c6, true).expect("v6"), v6);
}

#[test]
fn socket_io_endpoint_list() {
    use bitdaemon_bencode::{Entry, bdecode};
    use bitdaemon_net::socket_io::{endpoint_to_bytes, read_endpoint_list};
    use std::net::SocketAddr;

    // upstream: strings of 6 (v4) or 18 (v6) bytes in a list
    let a: SocketAddr = "10.0.0.1:51413".parse().unwrap_or_else(|_| unreachable!());
    let b: SocketAddr = "10.0.0.2:6881".parse().unwrap_or_else(|_| unreachable!());
    let e = Entry::Dict(
        [(
            b"peers".to_vec(),
            Entry::List(vec![
                Entry::String(endpoint_to_bytes(a)),
                Entry::String(endpoint_to_bytes(b)),
                Entry::String(vec![1, 2, 3]), // bad length → skipped
            ]),
        )]
        .into_iter()
        .collect(),
    );
    let buf = bitdaemon_bencode::bencode(&e);
    let node = bdecode(&buf);
    let peers = read_endpoint_list(&node.dict_find(b"peers"));
    assert_eq!(peers, vec![a, b]);
}

/// `write_dont_have` — upstream emits a BEP-10 `msg_extended` frame
/// under the negotiated `lt_donthave` id, never a bare `0x0f`.
#[test]
fn dont_have_encodes_as_extended() {
    let msg = PeerMessage::DontHave {
        piece: PieceIndex(5),
        ext_id: 7,
    };
    let m = encode(&msg);
    // upstream: `char msg[] = {0,0,0,6,msg_extended,dont_have_id,0,0,0,0}`
    // + `write_int32(index)` — len 6, id 20, ext id, u32 piece.
    assert_eq!(m.as_ref(), &[0, 0, 0, 6, 20, 7, 0, 0, 0, 5]);
    assert_eq!(bitdaemon_net::peer_wire::wire_len(&msg), m.len());
}

/// `on_have_none` upstream requires `packet_size == 1` — a `0x0f`
/// frame carrying a payload is `invalid_have_none` (disconnect).
#[test]
fn have_none_rejects_payload() {
    let mut codec = PeerWireCodec::default();
    // the OLD encoding — `0x0f` + 4-byte piece — must not decode.
    let mut buf = BytesMut::from(&[0, 0, 0, 5, 0x0f, 0, 0, 0, 9][..]);
    assert!(codec.decode(&mut buf).is_err());
    // len-1 still parses as HaveNone.
    let mut buf = BytesMut::from(&[0, 0, 0, 1, 0x0f][..]);
    assert_eq!(
        codec.decode(&mut buf).expect("decode").expect("frame"),
        PeerMessage::HaveNone
    );
}

/// an `lt_donthave` frame decodes as plain `Extended` — the session
/// layer dispatches on the negotiated id.
#[test]
fn lt_donthave_decodes_as_extended() {
    let mut codec = PeerWireCodec::default();
    let mut buf = BytesMut::from(&[0, 0, 0, 6, 20, 7, 0, 0, 0, 5][..]);
    assert_eq!(
        codec.decode(&mut buf).expect("decode").expect("frame"),
        PeerMessage::Extended {
            id: 7,
            data: bytes::Bytes::from_static(&[0, 0, 0, 5])
        }
    );
}
