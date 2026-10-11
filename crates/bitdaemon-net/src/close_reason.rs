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
        LtError::InvalidSwarmMetadata => CloseReason::InvalidMetadata,
        LtError::SessionIsClosing
        | LtError::DestructingTorrent
        | LtError::TorrentPaused
        | LtError::TorrentAborted
        | LtError::TorrentRemoved
        | LtError::TorrentNotReady
        | LtError::SessionClosing
        | LtError::StoppingTorrent => CloseReason::TorrentRemoved,
        LtError::PeerSentEmptyPiece | LtError::InvalidPiece | LtError::InvalidPieceSize => {
            CloseReason::InvalidPieceMessage
        }
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
        _ => CloseReason::None,
    }
}
