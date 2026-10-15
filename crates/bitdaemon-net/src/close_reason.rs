// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `close_reason.hpp` + `close_reason.cpp` —
//! `close_reason_t` (peer-disconnect cause sent on uTP close and
//! recorded by the socket layer) and `error_to_close_reason`, the
//! verbatim mapping from `lt::errors` codes to close reasons.
//!
//! Upstream carries this on `socket_type`/`utp_stream`; here the
//! enum is the canonical disconnect cause — `PeerDisconnected`
//! alert strings and `DisconnectPeer` effects can map onto it.

use bitdaemon_common::lt_error::LtError;

/// `close_reason_t` — discriminants verbatim (`u16`, the
/// `encryption_error` block starts at 256 upstream).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u16)]
#[allow(missing_docs)]
pub enum CloseReason {
    #[default]
    None = 0,
    DuplicatePeerId,
    TorrentRemoved,
    NoMemory,
    PortBlocked,
    Blocked,
    UploadToUpload,
    NotInterestedUploadOnly,
    Timeout,
    TimedOutInterest,
    TimedOutActivity,
    TimedOutHandshake,
    TimedOutRequest,
    ProtocolBlocked,
    PeerChurn,
    TooManyConnections,
    TooManyFiles,
    // --- protocol errors (uTP close-reason extension range) ---
    EncryptionError = 256,
    InvalidInfoHash,
    SelfConnection,
    InvalidMetadata,
    MetadataTooBig,
    MessageTooBig,
    InvalidMessageId,
    InvalidMessage,
    InvalidPieceMessage,
    InvalidHaveMessage,
    InvalidBitfieldMessage,
    InvalidChokeMessage,
    InvalidUnchokeMessage,
    InvalidInterestedMessage,
    InvalidNotInterestedMessage,
    InvalidRequestMessage,
    InvalidRejectMessage,
    InvalidAllowFastMessage,
    InvalidExtendedMessage,
    InvalidCancelMessage,
    InvalidDhtPortMessage,
    InvalidSuggestMessage,
    InvalidHaveAllMessage,
    InvalidDontHaveMessage,
    InvalidHaveNoneMessage,
    InvalidPexMessage,
    InvalidMetadataRequestMessage,
    InvalidMetadataMessage,
    InvalidMetadataOffset,
    RequestWhenChoked,
    CorruptPieces,
    PexMessageTooBig,
    PexTooFrequent,
}

/// `error_to_close_reason` — the verbatim `lt::errors` →
/// `close_reason_t` table. Non-libtorrent error categories map to
/// `none` upstream (they `return close_reason_t::none` at the end).
#[must_use]
pub fn error_to_close_reason(ec: LtError) -> CloseReason {
    match ec {
        LtError::DestructingTorrent
        | LtError::TorrentPaused
        | LtError::TorrentAborted
        | LtError::TorrentRemoved
        | LtError::TorrentNotReady
        | LtError::SessionClosing
        | LtError::StoppingTorrent => CloseReason::TorrentRemoved,
        LtError::InvalidPiece | LtError::InvalidPieceSize => CloseReason::InvalidPieceMessage,
        LtError::MismatchingInfoHash | LtError::InvalidInfoHash => CloseReason::InvalidInfoHash,
        LtError::PortBlocked | LtError::BannedByPortFilter => CloseReason::PortBlocked,
        LtError::TimedOut => CloseReason::Timeout,
        LtError::UploadUploadConnection | LtError::TorrentFinished => CloseReason::UploadToUpload,
        LtError::UninterestingUploadPeer => CloseReason::NotInterestedUploadOnly,
        LtError::InvalidHave => CloseReason::InvalidHaveMessage,
        LtError::InvalidBitfieldSize => CloseReason::InvalidBitfieldMessage,
        LtError::TooManyRequestsWhenChoked => CloseReason::RequestWhenChoked,
        LtError::NoMemory => CloseReason::NoMemory,
        LtError::SelfConnection => CloseReason::SelfConnection,
        LtError::TimedOutNoInterest => CloseReason::TimedOutInterest,
        LtError::TimedOutInactivity => CloseReason::TimedOutActivity,
        LtError::TimedOutNoHandshake => CloseReason::TimedOutHandshake,
        LtError::TimedOutNoRequest => CloseReason::TimedOutRequest,
        LtError::InvalidChoke => CloseReason::InvalidChokeMessage,
        LtError::InvalidUnchoke => CloseReason::InvalidUnchokeMessage,
        LtError::InvalidInterested => CloseReason::InvalidInterestedMessage,
        LtError::InvalidNotInterested => CloseReason::InvalidNotInterestedMessage,
        LtError::InvalidRequest => CloseReason::InvalidRequestMessage,
        LtError::InvalidHashList | LtError::InvalidHashPiece | LtError::InvalidLtTrackerMessage => {
            CloseReason::InvalidMessage
        }
        LtError::InvalidCancel => CloseReason::InvalidCancelMessage,
        LtError::InvalidDhtPort => CloseReason::InvalidDhtPortMessage,
        LtError::InvalidSuggest => CloseReason::InvalidSuggestMessage,
        LtError::InvalidHaveAll => CloseReason::InvalidHaveAllMessage,
        LtError::InvalidHaveNone => CloseReason::InvalidHaveNoneMessage,
        LtError::InvalidReject => CloseReason::InvalidRejectMessage,
        LtError::InvalidAllowFast => CloseReason::InvalidAllowFastMessage,
        LtError::InvalidExtended => CloseReason::InvalidExtendedMessage,
        LtError::InvalidMessage => CloseReason::InvalidMessageId,
        LtError::SyncHashNotFound
        | LtError::InvalidEncryptionConstant
        | LtError::InvalidPadSize
        | LtError::InvalidEncryptHandshake => CloseReason::EncryptionError,
        LtError::NoPlaintextMode
        | LtError::NoRc4Mode
        | LtError::UnsupportedEncryptionModeSelected
        | LtError::NoIncomingEncrypted
        | LtError::NoIncomingRegular
        | LtError::RequiresSslConnection => CloseReason::ProtocolBlocked,
        LtError::DuplicatePeerId => CloseReason::DuplicatePeerId,
        LtError::PacketTooLarge => CloseReason::MessageTooBig,
        LtError::OptimisticDisconnect => CloseReason::PeerChurn,
        LtError::TooManyCorruptPieces => CloseReason::CorruptPieces,
        LtError::TooManyConnections => CloseReason::TooManyConnections,
        LtError::PeerBanned | LtError::InvalidSslCert | LtError::NotAnSslTorrent => {
            CloseReason::Blocked
        }
        LtError::MetadataTooLarge | LtError::InvalidMetadataSize => CloseReason::MetadataTooBig,
        LtError::InvalidMetadataRequest => CloseReason::InvalidMetadataRequestMessage,
        LtError::InvalidMetadataOffset => CloseReason::InvalidMetadataOffset,
        LtError::InvalidMetadataMessage => CloseReason::InvalidMetadataMessage,
        LtError::PexMessageTooLarge => CloseReason::PexMessageTooBig,
        LtError::InvalidPexMessage => CloseReason::InvalidPexMessage,
        LtError::TooFrequentPex => CloseReason::PexTooFrequent,
        LtError::InvalidDontHave => CloseReason::InvalidDontHaveMessage,
        // upstream maps every `http_category` (status-code) error to
        // `no_memory`; `LtError` has no http-category variant — the
        // `http_errors` enum lives in `http_parser`'s error space.
        // `errors::http_error` itself is lt-category and unmapped → none.
        _ => CloseReason::None,
    }
}

/// `reason` is the upstream error *name* carried on our
/// `DisconnectPeer`/`PeerGone` paths (e.g. `"self_connection"`,
/// `"too_many_connections"`) — mapped to the same `close_reason_t`
/// `disconnect(ec, …)` would send on the wire. Unmapped names return
/// `None` (upstream's catch-all).
#[must_use]
pub fn reason_to_close_reason(reason: &str) -> CloseReason {
    match reason {
        "self_connection" => CloseReason::SelfConnection,
        "duplicate_peer_id" | "duplicate peer id" => CloseReason::DuplicatePeerId,
        "invalid_info_hash" | "mismatching_info_hash" => CloseReason::InvalidInfoHash,
        "too_many_connections" => CloseReason::TooManyConnections,
        "banned_by_port_filter" | "port_filter" => CloseReason::PortBlocked,
        "banned_by_ip_filter"
        | "ip_filter"
        | "peer_banned"
        | "address_filter"
        | "requires_ssl_connection"
        | "invalid_ssl_cert"
        | "not_an_ssl_torrent" => CloseReason::Blocked,
        "tcp_disabled" | "utp_disabled" | "invalid_local_interface" | "protocol_blocked" => {
            CloseReason::ProtocolBlocked
        }
        "torrent_paused" | "torrent_removed" | "torrent_aborted" | "session_closing"
        | "stopping_torrent" | "torrent_not_ready" | "clear_peers" => CloseReason::TorrentRemoved,
        "upload-upload" | "upload_upload_connection" | "torrent finished" | "torrent_finished" => {
            CloseReason::UploadToUpload
        }
        "uninteresting_upload_peer" => CloseReason::NotInterestedUploadOnly,
        "timed_out" => CloseReason::Timeout,
        "timed_out_no_interest" => CloseReason::TimedOutInterest,
        "timed_out_inactivity" => CloseReason::TimedOutActivity,
        "timed_out_no_handshake" => CloseReason::TimedOutHandshake,
        "timed_out_no_request" => CloseReason::TimedOutRequest,
        "invalid_have" | "invalid have index" => CloseReason::InvalidHaveMessage,
        "invalid_bitfield_size" | "invalid_bitfield" | "invalid bitfield" => {
            CloseReason::InvalidBitfieldMessage
        }
        "too_many_requests_when_choked" | "request_when_choked" => CloseReason::RequestWhenChoked,
        "out of memory" | "no_memory" => CloseReason::NoMemory,
        "invalid_choke" | "invalid choke" => CloseReason::InvalidChokeMessage,
        "invalid_unchoke" | "invalid unchoke" => CloseReason::InvalidUnchokeMessage,
        "invalid_interested" => CloseReason::InvalidInterestedMessage,
        "invalid_not_interested" => CloseReason::InvalidNotInterestedMessage,
        "invalid_request" | "invalid request" | "invalid piece index" | "invalid piece size" => {
            CloseReason::InvalidRequestMessage
        }
        "invalid piece" | "invalid_piece" | "invalid_piece_size" => {
            CloseReason::InvalidPieceMessage
        }
        "invalid_cancel" | "invalid cancel" => CloseReason::InvalidCancelMessage,
        "invalid_dht_port" | "invalid dht port" => CloseReason::InvalidDhtPortMessage,
        "invalid_suggest" | "invalid suggest" => CloseReason::InvalidSuggestMessage,
        "invalid_have_all" => CloseReason::InvalidHaveAllMessage,
        "invalid_have_none" => CloseReason::InvalidHaveNoneMessage,
        "invalid_reject" | "invalid reject" => CloseReason::InvalidRejectMessage,
        "invalid_allow_fast" | "invalid allow fast" => CloseReason::InvalidAllowFastMessage,
        "invalid_extended" | "invalid extended" => CloseReason::InvalidExtendedMessage,
        "invalid_message" | "invalid message id" => CloseReason::InvalidMessageId,
        "packet_too_large" | "message_too_big" => CloseReason::MessageTooBig,
        "optimistic_disconnect" => CloseReason::PeerChurn,
        "too_many_corrupt_pieces" | "smart_ban: corrupt block" => CloseReason::CorruptPieces,
        "metadata_too_large" | "invalid_metadata_size" => CloseReason::MetadataTooBig,
        "invalid_metadata_request" => CloseReason::InvalidMetadataRequestMessage,
        "invalid_metadata_offset" => CloseReason::InvalidMetadataOffset,
        "invalid_metadata_message" | "invalid_metadata" => CloseReason::InvalidMetadataMessage,
        "pex message too large" | "pex_message_too_large" => CloseReason::PexMessageTooBig,
        "invalid pex message" | "invalid_pex_message" => CloseReason::InvalidPexMessage,
        "too frequent pex" | "too_frequent_pex" => CloseReason::PexTooFrequent,
        "invalid_dont_have" => CloseReason::InvalidDontHaveMessage,
        _ => CloseReason::None,
    }
}
