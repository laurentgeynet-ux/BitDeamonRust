// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `test_alert_types.cpp` — the upstream conformance table
//! `(alert_type, priority, static_category)` applied to every `Alert`
//! variant. Guards the enum layout against drift (ADR-0023 D1/D2).

use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};

use bitdaemon_common::Sha1Hash;
use bitdaemon_common::types::PieceIndex;
use bitdaemon_session::alerts::{
    Alert, AlertCategory, AlertPriority, DhtMutableItemAlert, DhtNodeStats, DhtPutAlert, TrackerOp,
};

const E: AlertCategory = AlertCategory::ERROR;
const PEER: AlertCategory = AlertCategory::PEER;
const PM: AlertCategory = AlertCategory::PORT_MAPPING;
const ST: AlertCategory = AlertCategory::STORAGE;
const TR: AlertCategory = AlertCategory::TRACKER;
const CO: AlertCategory = AlertCategory::CONNECT;
const S: AlertCategory = AlertCategory::STATUS;
const IPB: AlertCategory = AlertCategory::IP_BLOCK;
const PERF: AlertCategory = AlertCategory::PERFORMANCE;
const DHT: AlertCategory = AlertCategory::DHT;
const STATS: AlertCategory = AlertCategory::STATS;
const SLOG: AlertCategory = AlertCategory::SESSION_LOG;
const TLOG: AlertCategory = AlertCategory::TORRENT_LOG;
const PLOG: AlertCategory = AlertCategory::PEER_LOG;
const IREQ: AlertCategory = AlertCategory::INCOMING_REQUEST;
const DLOG: AlertCategory = AlertCategory::DHT_LOG;
const DOP: AlertCategory = AlertCategory::DHT_OPERATION;
const PMLOG: AlertCategory = AlertCategory::PORT_MAPPING_LOG;
const PKLOG: AlertCategory = AlertCategory::PICKER_LOG;
const FPROG: AlertCategory = AlertCategory::FILE_PROGRESS;
const PPROG: AlertCategory = AlertCategory::PIECE_PROGRESS;
const UPL: AlertCategory = AlertCategory::UPLOAD;
const BPROG: AlertCategory = AlertCategory::BLOCK_PROGRESS;
const ZERO: AlertCategory = AlertCategory(0);

const N: AlertPriority = AlertPriority::Normal;
const H: AlertPriority = AlertPriority::High;
const C: AlertPriority = AlertPriority::Critical;
const M: AlertPriority = AlertPriority::Meta;

const fn or(a: AlertCategory, b: AlertCategory) -> AlertCategory {
    AlertCategory(a.0 | b.0)
}
const fn or3(a: AlertCategory, b: AlertCategory, c: AlertCategory) -> AlertCategory {
    AlertCategory(a.0 | b.0 | c.0)
}

fn ih() -> Sha1Hash {
    Sha1Hash::from_bytes(&[1; 20]).unwrap()
}
fn ep() -> String {
    "1.2.3.4:6881".to_string()
}
fn sa() -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6881))
}

/// One `(alert, type, priority, category)` row per upstream
/// `TEST_ALERT_TYPE(name, seq, prio, cat)` — same order, same ids.
/// `read_piece_failed` shares type 5 with `read_piece_alert`
/// (upstream sets `ec` on the same alert; we split the variants).
fn table() -> Vec<(Alert, u32, AlertPriority, AlertCategory)> {
    vec![
        (Alert::TorrentAdded(ih()), 3, N, S),
        (Alert::TorrentRemoved(ih()), 4, C, S),
        (
            Alert::ReadPiece {
                ih: ih(),
                piece: 0,
                buffer: vec![],
            },
            5,
            C,
            ST,
        ),
        (
            Alert::ReadPieceFailed {
                ih: ih(),
                piece: 0,
                error: "e".into(),
            },
            5,
            C,
            ST,
        ),
        (Alert::FileCompleted { ih: ih(), index: 0 }, 6, N, FPROG),
        (
            Alert::FileRenamed {
                ih: ih(),
                index: 0,
                name: "n".into(),
                old_name: "o".into(),
            },
            7,
            C,
            ST,
        ),
        (
            Alert::FileRenameFailed {
                ih: ih(),
                index: 0,
                error: "e".into(),
            },
            8,
            C,
            ST,
        ),
        (
            Alert::Performance {
                warning: "w".into(),
            },
            9,
            N,
            PERF,
        ),
        (
            Alert::StateChanged {
                ih: ih(),
                prev: "a".into(),
                next: "b".into(),
            },
            10,
            H,
            S,
        ),
        (
            Alert::TrackerError {
                ih: ih(),
                url: "u".into(),
                error: "e".into(),
                times_in_row: 1,
                status_code: -1,
                op: TrackerOp::TimedOut,
            },
            11,
            H,
            or(TR, E),
        ),
        (
            Alert::TrackerWarning {
                ih: ih(),
                url: "u".into(),
                msg: "m".into(),
            },
            12,
            N,
            or(TR, E),
        ),
        (
            Alert::ScrapeReply {
                ih: ih(),
                url: "u".into(),
                complete: 1,
                incomplete: 2,
            },
            13,
            C,
            TR,
        ),
        (
            Alert::ScrapeFailed {
                ih: ih(),
                url: "u".into(),
                reason: "r".into(),
            },
            14,
            C,
            or(TR, E),
        ),
        (
            Alert::TrackerReply {
                ih: ih(),
                url: "u".into(),
                num_peers: 0,
            },
            15,
            N,
            TR,
        ),
        (
            Alert::DhtReply {
                ih: ih(),
                url: "u".into(),
                num_peers: 0,
            },
            16,
            N,
            or(DHT, TR),
        ),
        (
            Alert::TrackerAnnounce {
                ih: ih(),
                url: "u".into(),
                event: 0,
            },
            17,
            N,
            TR,
        ),
        (
            Alert::HashFailed {
                ih: ih(),
                piece: PieceIndex(0),
            },
            18,
            N,
            S,
        ),
        (
            Alert::PeerBan {
                ih: ih(),
                ip: sa(),
                pid: None,
            },
            19,
            N,
            PEER,
        ),
        (
            Alert::PeerUnsnubbed {
                ih: ih(),
                remote: ep(),
            },
            20,
            N,
            PEER,
        ),
        (
            Alert::PeerSnubbed {
                ih: ih(),
                remote: ep(),
            },
            21,
            N,
            PEER,
        ),
        (
            Alert::PeerError {
                ih: ih(),
                remote: ep(),
                op: "o".into(),
                error: "e".into(),
            },
            22,
            N,
            PEER,
        ),
        (
            Alert::PeerConnect {
                ih: ih(),
                remote: ep(),
            },
            23,
            N,
            CO,
        ),
        (
            Alert::PeerDisconnected {
                ih: ih(),
                reason: "r".into(),
            },
            24,
            N,
            CO,
        ),
        (
            Alert::InvalidRequest {
                ih: ih(),
                remote: ep(),
                msg: "m".into(),
            },
            25,
            N,
            PEER,
        ),
        (Alert::TorrentFinished(ih()), 26, H, S),
        (
            Alert::PieceFinished {
                ih: ih(),
                piece: PieceIndex(0),
            },
            27,
            N,
            PPROG,
        ),
        (
            Alert::RequestDropped {
                ih: ih(),
                remote: ep(),
                piece: PieceIndex(0),
                block: 0,
            },
            28,
            N,
            or(BPROG, PEER),
        ),
        (
            Alert::BlockTimeout {
                ih: ih(),
                remote: ep(),
                piece: PieceIndex(0),
                block: 0,
            },
            29,
            N,
            or(BPROG, PEER),
        ),
        (
            Alert::BlockFinished {
                ih: ih(),
                piece: PieceIndex(0),
                block: 0,
            },
            30,
            N,
            BPROG,
        ),
        (
            Alert::BlockDownloading {
                ih: ih(),
                remote: ep(),
                piece: PieceIndex(0),
                block: 0,
            },
            31,
            N,
            BPROG,
        ),
        (
            Alert::UnwantedBlock {
                ih: ih(),
                remote: ep(),
                piece: PieceIndex(0),
                block: 0,
            },
            32,
            N,
            PEER,
        ),
        (
            Alert::StorageMoved {
                ih: ih(),
                path: "p".into(),
                old_path: "q".into(),
            },
            33,
            C,
            ST,
        ),
        (
            Alert::StorageMovedFailed {
                ih: ih(),
                file: "f".into(),
                op: "file_rename".into(),
                error: "e".into(),
            },
            34,
            C,
            ST,
        ),
        (Alert::TorrentDeleted(ih()), 35, C, ST),
        (
            Alert::TorrentDeleteFailed {
                ih: ih(),
                error: "e".into(),
            },
            36,
            C,
            or(ST, E),
        ),
        (
            Alert::SaveResumeData {
                ih: ih(),
                data: vec![],
            },
            37,
            C,
            ST,
        ),
        (
            Alert::SaveResumeDataFailed {
                ih: ih(),
                error: "e".into(),
            },
            38,
            C,
            or(ST, E),
        ),
        (Alert::TorrentPaused { ih: ih() }, 39, H, S),
        (Alert::TorrentResumed { ih: ih() }, 40, H, S),
        (Alert::TorrentChecked(ih()), 41, H, S),
        (
            Alert::UrlSeed {
                ih: ih(),
                url: "u".into(),
                msg: "m".into(),
            },
            42,
            N,
            or(PEER, E),
        ),
        (
            Alert::FileError {
                ih: ih(),
                path: "p".into(),
                op: "file_stat".into(),
                error: "e".into(),
            },
            43,
            H,
            or3(S, E, ST),
        ),
        (
            Alert::MetadataFailed {
                ih: ih(),
                error: "e".into(),
            },
            44,
            N,
            E,
        ),
        (Alert::MetadataReceived { ih: ih() }, 45, N, S),
        (
            Alert::UdpError {
                remote: ep(),
                op: "o".into(),
                error: "e".into(),
            },
            46,
            N,
            E,
        ),
        (
            Alert::ExternalIp {
                addr: IpAddr::V4(Ipv4Addr::LOCALHOST),
            },
            47,
            N,
            S,
        ),
        (
            Alert::ListenFailed {
                remote: ep(),
                op: "o".into(),
                error: "e".into(),
                kind: 0,
            },
            48,
            C,
            or(S, E),
        ),
        (
            Alert::ListenSucceeded {
                remote: ep(),
                kind: 0,
            },
            49,
            C,
            S,
        ),
        (
            Alert::PortmapError {
                mapping: 0,
                transport: 0,
                local_addr: "l".into(),
                error: "e".into(),
            },
            50,
            N,
            or(PM, E),
        ),
        (
            Alert::Portmap {
                mapping: 0,
                transport: 0,
                protocol: 1,
                ext_port: 6881,
                local_addr: "l".into(),
            },
            51,
            N,
            PM,
        ),
        (
            Alert::PortmapLog {
                transport: 0,
                local_addr: "l".into(),
                msg: "m".into(),
            },
            52,
            N,
            PMLOG,
        ),
        (
            Alert::FastresumeRejected {
                ih: ih(),
                error: "e".into(),
                op: "o".into(),
                file: "f".into(),
            },
            53,
            C,
            or(S, E),
        ),
        (
            Alert::PeerBlocked {
                ih: ih(),
                remote: ep(),
                reason: "r".into(),
            },
            54,
            N,
            IPB,
        ),
        (
            Alert::DhtAnnounce {
                remote: ep(),
                port: 6881,
                ih: ih(),
                msg: "m".into(),
            },
            55,
            N,
            DHT,
        ),
        (
            Alert::DhtGetPeers {
                ih: ih(),
                msg: "m".into(),
            },
            56,
            N,
            DHT,
        ),
        (
            Alert::Stats {
                ih: ih(),
                interval_ms: 0,
                counters: vec![],
            },
            57,
            N,
            STATS,
        ),
        (Alert::CacheFlushed(ih()), 58, H, ST),
        (
            Alert::AnonymousMode {
                ih: ih(),
                kind: 0,
                msg: "m".into(),
            },
            59,
            N,
            E,
        ),
        (
            Alert::LsdPeer {
                ih: ih(),
                remote: ep(),
            },
            60,
            N,
            PEER,
        ),
        (
            Alert::Trackerid {
                ih: ih(),
                url: "u".into(),
                trackerid: "t".into(),
            },
            61,
            N,
            S,
        ),
        (Alert::DhtBootstrap {}, 62, N, DHT),
        (
            Alert::TorrentError {
                ih: ih(),
                error: "e".into(),
                file: "f".into(),
            },
            64,
            H,
            or(E, S),
        ),
        (
            Alert::TorrentNeedCert {
                ih: ih(),
                error: "e".into(),
                cert_file: "c".into(),
            },
            65,
            C,
            S,
        ),
        (
            Alert::IncomingConnection {
                remote: ep(),
                kind: 0,
            },
            66,
            N,
            PEER,
        ),
        (
            Alert::AddTorrent {
                ih: ih(),
                error: String::new(),
            },
            67,
            C,
            S,
        ),
        (Alert::StateUpdate { statuses: vec![] }, 68, H, S),
        (
            Alert::SessionStats {
                stats: bitdaemon_session::counters::SessionStats::default(),
            },
            70,
            C,
            ZERO,
        ),
        (
            Alert::DhtError {
                op: 0,
                error: "e".into(),
            },
            73,
            N,
            or(E, DHT),
        ),
        (
            Alert::DhtImmutableItem {
                target: ih(),
                value: vec![],
            },
            74,
            C,
            DHT,
        ),
        (
            Alert::DhtMutableItem(Box::new(DhtMutableItemAlert {
                target: ih(),
                key: [0; 32],
                signature: [0; 64],
                seq: 0,
                salt: vec![],
                value: vec![],
                authoritative: false,
            })),
            75,
            C,
            DHT,
        ),
        (
            Alert::DhtPut(Box::new(DhtPutAlert {
                target: ih(),
                num_success: 0,
                public_key: [0; 32],
                signature: [0; 64],
                seq: 0,
                salt: vec![],
            })),
            76,
            N,
            DHT,
        ),
        (Alert::I2p { error: "e".into() }, 77, N, E),
        (
            Alert::DhtOutgoingGetPeers {
                ih: ih(),
                obfuscated: ih(),
                ep: "1.2.3.4:6881".parse().unwrap(),
            },
            78,
            N,
            DHT,
        ),
        (Alert::Log { msg: "m".into() }, 79, N, SLOG),
        (
            Alert::TorrentLog {
                ih: ih(),
                msg: "m".into(),
            },
            80,
            N,
            TLOG,
        ),
        (
            Alert::PeerLog {
                ih: ih(),
                remote: ep(),
                msg: "m".into(),
            },
            81,
            N,
            PLOG,
        ),
        (Alert::LsdError { error: "e".into() }, 82, N, E),
        (
            Alert::DhtStats {
                v4: Box::new(DhtNodeStats::default()),
                v6: Box::new(DhtNodeStats::default()),
            },
            83,
            N,
            ZERO,
        ),
        (
            Alert::IncomingRequest {
                ih: ih(),
                remote: ep(),
                piece: PieceIndex(0),
                start: 0,
                length: 0,
            },
            84,
            N,
            IREQ,
        ),
        (
            Alert::DhtLog {
                module: 0,
                msg: "m".into(),
            },
            85,
            N,
            DLOG,
        ),
        (
            Alert::DhtPacket {
                direction: 0,
                remote: ep(),
                msg: vec![],
            },
            86,
            N,
            DLOG,
        ),
        (
            Alert::DhtGetPeersReply {
                ih: ih(),
                num_peers: 0,
                peers: vec![],
            },
            87,
            N,
            DOP,
        ),
        (
            Alert::DhtDirectResponse {
                remote: ep(),
                user_data: 0,
                response: vec![],
            },
            88,
            C,
            DHT,
        ),
        (
            Alert::PickerLog {
                ih: ih(),
                picker_flags: 0,
                blocks: vec![],
            },
            89,
            N,
            PKLOG,
        ),
        (Alert::SessionError("e".into()), 90, N, E),
        (
            Alert::DhtLiveNodes {
                ih: ih(),
                nodes: vec![],
            },
            91,
            N,
            DHT,
        ),
        (Alert::SessionStatsHeader {}, 92, N, ZERO),
        (
            Alert::DhtSampleInfohashes(Box::new(
                bitdaemon_session::alerts::DhtSampleInfohashesAlert {
                    node_id: ih(),
                    remote: "1.2.3.4:6881".into(),
                    interval_secs: 0,
                    num: 0,
                    samples: vec![],
                    nodes: vec![],
                },
            )),
            93,
            N,
            DOP,
        ),
        (
            Alert::BlockUploaded {
                ih: ih(),
                remote: ep(),
                piece: PieceIndex(0),
                block: 0,
            },
            94,
            N,
            UPL,
        ),
        (Alert::AlertsDropped { types: vec![] }, 95, M, E),
        (
            Alert::Socks5 {
                remote: ep(),
                op: "o".into(),
                error: "e".into(),
            },
            96,
            N,
            E,
        ),
        (
            Alert::FilePrio {
                ih: ih(),
                op: "o".into(),
                error: String::new(),
            },
            97,
            N,
            ST,
        ),
        (
            Alert::OversizedFile {
                ih: ih(),
                file: "f".into(),
            },
            98,
            N,
            ST,
        ),
        (
            Alert::TorrentConflict {
                ih: ih(),
                other: None,
                url: "u".into(),
            },
            99,
            H,
            E,
        ),
        (
            Alert::PeerInfo {
                ih: ih(),
                peers: vec![],
            },
            100,
            C,
            S,
        ),
        (
            Alert::FileProgress {
                ih: ih(),
                files: vec![],
            },
            101,
            C,
            FPROG,
        ),
        (
            Alert::PieceInfo {
                ih: ih(),
                queue: vec![],
            },
            102,
            C,
            PPROG,
        ),
        (
            Alert::PieceAvailability {
                ih: ih(),
                availability: vec![],
            },
            103,
            C,
            S,
        ),
        (
            Alert::TrackerList {
                ih: ih(),
                trackers: vec![],
            },
            104,
            C,
            S,
        ),
        (
            Alert::FilePriorities {
                ih: ih(),
                priorities: vec![],
            },
            105,
            C,
            S,
        ),
        (
            Alert::FileStatus {
                ih: ih(),
                state: vec![],
            },
            106,
            C,
            S,
        ),
        (
            Alert::IpBan {
                remote: ep(),
                reason: "r".into(),
            },
            107,
            N,
            IPB,
        ),
    ]
}

#[test]
fn alert_types_match_upstream() {
    let t = table();
    // 101 concrete upstream alert types (ids 3..107 minus the 4
    // removed slots 63/69/71/72) + our `ReadPieceFailed` split
    // sharing id 5 — upstream ends `count_alert_types` at 108.
    assert_eq!(t.len(), 102);
    let mut seen = std::collections::BTreeSet::new();
    for (a, ty, prio, cat) in &t {
        assert_eq!(a.alert_type(), *ty, "type mismatch for {:?}", a);
        assert_eq!(a.priority(), *prio, "priority mismatch for {:?}", a);
        assert_eq!(a.category(), *cat, "category mismatch for {:?}", a);
        seen.insert(*ty);
    }
    // 101 distinct upstream ids (5 appears twice via the split)
    assert_eq!(seen.len(), 101);
}

/// `alert_mask` default = `error` (upstream `settings_pack`); a
/// `mask = 0` session still receives category-0 alerts.
#[test]
fn default_mask_is_error_only() {
    use bitdaemon_session::alerts::AlertManager;
    let mut m = AlertManager::default();
    m.post(Alert::TorrentFinished(ih())); // STATUS — dropped
    m.post(Alert::SessionError("e".into())); // ERROR — lands
    m.post(Alert::SessionStatsHeader {}); // cat-0 — lands
    let all = m.get_all();
    assert_eq!(all.len(), 2);
}

/// `session_params` round-trips `alert_mask` through the settings dict.
#[test]
fn alert_mask_persists_in_session_params() {
    use bitdaemon_session::session_params::{read_session_params, write_session_params_buf};
    use bitdaemon_session::settings::SessionParams;
    let mut p = SessionParams::default();
    p.settings.alert_mask = AlertCategory::DHT.or(AlertCategory::ERROR);
    let bytes = write_session_params_buf(&p);
    let q = read_session_params(&bytes);
    assert_eq!(q.settings.alert_mask, p.settings.alert_mask);
}
