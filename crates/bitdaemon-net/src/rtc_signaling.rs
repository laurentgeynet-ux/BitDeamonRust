// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `aux_/rtc_signaling.{hpp,cpp}` — WebRTC offer/answer
//! negotiation for `webtorrent` peers. Upstream drives libdatachannel
//! `rtc::PeerConnection`s; here the stack is injected
//! ([`RtcPeerConnectionFactory`], ADR-0008) so OnionBit lanes or a
//! `webrtc`/`str0m` binding can plug in. The state machine — offer
//! batches, incoming-offer cap, dedup, negotiation timeout — is
//! ported verbatim.

use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bitdaemon_common::peer_id::PeerId;

use crate::rtc_stream::{RtcChannel, RtcInbound};

/// `RTC_OFFER_ID_LEN`.
pub const RTC_OFFER_ID_LEN: usize = 20;
/// `RTC_MAX_SDP_SIZE`.
pub const RTC_MAX_SDP_SIZE: usize = 64 * 1024;
/// `RTC_MAX_MESSAGE_SIZE`.
pub const RTC_MAX_MESSAGE_SIZE: usize = 2 * RTC_MAX_SDP_SIZE;

/// `rtc_offer_id` — random 20-byte offer tag.
pub type RtcOfferId = [u8; RTC_OFFER_ID_LEN];

/// `rtc_offer` — an SDP offer we send to a peer (or that arrived).
pub struct RtcOffer {
    /// `id`.
    pub id: RtcOfferId,
    /// `pid` — sender peer-id.
    pub pid: PeerId,
    /// `sdp` — session description in SDP format.
    pub sdp: String,
    /// `answer_callback` — invoked with the answer we generate
    /// (present only on incoming offers; upstream field verbatim).
    pub answer_callback: Option<Box<dyn FnOnce(PeerId, RtcAnswer) + Send>>,
}

impl Debug for RtcOffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RtcOffer")
            .field("id", &self.id)
            .field("pid", &self.pid)
            .field("sdp", &self.sdp)
            .finish_non_exhaustive()
    }
}

/// `rtc_answer`.
#[derive(Debug)]
pub struct RtcAnswer {
    /// `offer_id` the answer belongs to.
    pub offer_id: RtcOfferId,
    /// `pid` — answering peer-id.
    pub pid: PeerId,
    /// `sdp` — session description in SDP format.
    pub sdp: String,
}

/// SDP description kind (`"offer"`/`"answer"` upstream).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SdpKind {
    /// `setRemoteDescription({sdp, "offer"})`.
    Offer,
    /// `setRemoteDescription({sdp, "answer"})`.
    Answer,
}

/// Events the injected WebRTC stack reports for a connection
/// (libdatachannel callbacks upstream, posted onto the io_context —
/// here they are fed straight into [`RtcSignaling::on_event`]).
#[derive(Debug)]
pub enum RtcEvent {
    /// `onGatheringStateChange(Complete)` — local SDP is ready.
    GatheringComplete(String),
    /// `onStateChange(Failed)` — negotiation failed.
    Failed,
    /// `onDataChannel` / `dc->onOpen` — channel established
    /// (payload is the ready channel).
    DataChannel(Arc<dyn RtcChannel>),
}

/// Injected `rtc::PeerConnection` — one per offer/answer exchange.
pub trait RtcPeerConnection: Send + Debug {
    /// `pc->createDataChannel(label)` — creates the outgoing channel.
    fn create_data_channel(&mut self, label: &str);
    /// `pc->setRemoteDescription({sdp, kind})` — returns `Err` when
    /// the description is rejected (upstream throws). The local SDP
    /// (`pc->localDescription()`) is delivered back via
    /// [`RtcEvent::GatheringComplete`] once ICE gathering finishes.
    fn set_remote_description(&mut self, sdp: &str, kind: SdpKind) -> Result<(), String>;
}

/// `rtc::PeerConnection` constructor — `config.iceServers` comes
/// from `webtorrent_stun_server`.
pub trait RtcPeerConnectionFactory: Send + Sync + Debug + 'static {
    /// `std::make_shared<rtc::PeerConnection>(config)`.
    fn create(&self, stun_server: &str) -> Box<dyn RtcPeerConnection>;
}

/// Output of the signaling machine (upstream posts these onto the
/// io_context; we expose them via [`RtcSignaling::poll_out`]).
#[derive(Debug)]
pub enum RtcSigOut {
    /// A completed `offers_handler` batch — `Vec` length is the
    /// number of offers actually generated (failed ones are
    /// dropped, upstream `offer_batch::add` semantics).
    Offers(Vec<RtcOffer>),
    /// An incoming offer was answered — the caller invokes the
    /// offer's `answer_callback`.
    AnswerGenerated {
        /// the offer this answers (carries `answer_callback`).
        offer: RtcOffer,
        /// the generated answer.
        answer: RtcAnswer,
    },
    /// `rtc_stream_init{pc, dc}` — channel open, wrap in
    /// `RtcStream` via the returned inbound mailbox.
    StreamReady {
        /// the data channel (`dc` upstream).
        channel: Arc<dyn RtcChannel>,
        /// feed inbound messages into this mailbox
        /// (`RtcStream::feed`).
        inbound: Arc<Mutex<RtcInbound>>,
    },
}

/// `rtc_signaling::connection`.
#[derive(Debug)]
struct Connection {
    pc: Box<dyn RtcPeerConnection>,
    pid: Option<PeerId>,
    incoming: bool,
    /// `timer` — negotiation deadline.
    deadline: Instant,
}

/// `rtc_signaling::offer_batch`.
struct OfferBatch {
    remaining: i32,
    offers: Vec<RtcOffer>,
}

impl OfferBatch {
    /// `is_complete()`.
    fn is_complete(&self) -> bool {
        self.offers.len() as i32 == self.remaining
    }
    /// `add(ec, offer)` — errors decrement the expected count.
    fn add(&mut self, offer: Option<RtcOffer>) {
        match offer {
            Some(o) => self.offers.push(o),
            None => self.remaining -= 1,
        }
    }
}

/// `rtc_signaling` — per-torrent WebRTC offer/answer state machine.
pub struct RtcSignaling {
    factory: Arc<dyn RtcPeerConnectionFactory>,
    /// `m_torrent->pid()`.
    pid: PeerId,
    stun_server: String,
    connection_timeout: Duration,
    max_incoming: i32,
    /// `m_connections`.
    connections: HashMap<RtcOfferId, Connection>,
    /// `m_num_incoming_connections`.
    num_incoming: i32,
    /// `m_offer_batches`.
    offer_batches: VecDeque<OfferBatch>,
    /// incoming offers kept for their `answer_callback`.
    pending_offers: HashMap<RtcOfferId, RtcOffer>,
    outbox: VecDeque<RtcSigOut>,
}

impl Debug for RtcSignaling {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RtcSignaling")
            .field("connections", &self.connections.len())
            .field("num_incoming", &self.num_incoming)
            .finish_non_exhaustive()
    }
}

impl RtcSignaling {
    /// `rtc_signaling(ioc, torrent, handler)`.
    pub fn new(
        factory: Arc<dyn RtcPeerConnectionFactory>,
        pid: PeerId,
        stun_server: String,
        connection_timeout: Duration,
        max_webtorrent_offers: i32,
    ) -> Self {
        Self {
            factory,
            pid,
            stun_server,
            connection_timeout: connection_timeout.max(Duration::from_secs(1)),
            max_incoming: max_webtorrent_offers.max(0),
            connections: HashMap::new(),
            num_incoming: 0,
            offer_batches: VecDeque::new(),
            pending_offers: HashMap::new(),
            outbox: VecDeque::new(),
        }
    }

    /// `close()` — drop all pending connections.
    pub fn close(&mut self) {
        self.connections.clear();
        self.num_incoming = 0;
    }

    /// `generate_offer_id()`.
    fn generate_offer_id() -> RtcOfferId {
        let mut id = [0u8; RTC_OFFER_ID_LEN];
        bitdaemon_common::random::crypto_random_bytes(&mut id);
        id
    }

    /// `generate_offers(count, handler)` — batches complete into
    /// [`RtcSigOut::Offers`].
    pub fn generate_offers(&mut self, count: i32, now: Instant) {
        self.offer_batches.push_back(OfferBatch {
            remaining: count.max(0),
            offers: Vec::new(),
        });
        // count==0 upstream fires the handler immediately
        if count <= 0 {
            self.flush_batches();
            return;
        }
        for _ in 0..count {
            let offer_id = Self::generate_offer_id();
            let conn = self.create_connection(offer_id, now);
            conn.pc.create_data_channel("webtorrent");
        }
    }

    /// `process_offer(offer)` — remote offer arrived.
    pub fn process_offer(&mut self, offer: RtcOffer, now: Instant) {
        if self.connections.contains_key(&offer.id) {
            return; // duplicate — likely our own offer
        }
        if self.num_incoming >= self.max_incoming {
            return; // connection limit reached
        }
        {
            let conn = self.create_connection(offer.id, now);
            conn.pid = Some(offer.pid);
            conn.incoming = true;
        }
        self.num_incoming += 1;
        let rejected = self.connections.get_mut(&offer.id).is_some_and(|c| {
            c.pc.set_remote_description(&offer.sdp, SdpKind::Offer)
                .is_err()
        });
        if rejected {
            self.num_incoming -= 1;
            self.connections.remove(&offer.id);
            return;
        }
        // keep the offer for its answer_callback
        self.pending_offers.insert(offer.id, offer);
    }

    /// `process_answer(answer)` — answer to one of our offers.
    pub fn process_answer(&mut self, answer: &RtcAnswer) {
        let Some(conn) = self.connections.get_mut(&answer.offer_id) else {
            return; // unknown offer
        };
        if conn.pid.is_some() {
            return; // offer already answered
        }
        conn.pid = Some(answer.pid);
        if conn
            .pc
            .set_remote_description(&answer.sdp, SdpKind::Answer)
            .is_err()
        {
            self.connections.remove(&answer.offer_id);
        }
    }

    /// Event delivery — the injected stack posts libdatachannel
    /// callbacks here (`post(io_context, …)` upstream).
    pub fn on_event(&mut self, offer_id: RtcOfferId, ev: RtcEvent) {
        match ev {
            RtcEvent::GatheringComplete(sdp) => {
                if self.pending_offers.contains_key(&offer_id) {
                    // incoming connection → this SDP is the answer
                    self.on_generated_answer(offer_id, sdp);
                } else {
                    self.on_generated_offer(offer_id, sdp);
                }
            }
            RtcEvent::Failed => self.on_data_channel(offer_id, None),
            RtcEvent::DataChannel(dc) => self.on_data_channel(offer_id, Some(dc)),
        }
    }

    /// `timer` expiry — call on the session tick. Unlike the
    /// `Failed` path, a timeout only *drops* the connection:
    /// upstream's `on_data_channel` never touches the offer batch
    /// (only the description handler — [`RtcEvent::Failed`] —
    /// decrements it), so an expired offer leaves the batch open.
    pub fn tick(&mut self, now: Instant) {
        let expired: Vec<RtcOfferId> = self
            .connections
            .iter()
            .filter(|(_, c)| c.deadline <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            self.remove_connection(id);
        }
    }

    /// `on_data_channel` conn removal, without the batch accounting
    /// (upstream only erases + decrements `m_num_incoming_connections`).
    fn remove_connection(&mut self, offer_id: RtcOfferId) {
        if let Some(conn) = self.connections.remove(&offer_id) {
            if conn.incoming {
                self.num_incoming -= 1;
            }
        }
        self.pending_offers.remove(&offer_id);
    }

    /// Drain completed outputs (offers/answers/ready channels).
    pub fn poll_out(&mut self) -> Option<RtcSigOut> {
        self.outbox.pop_front()
    }

    /// `create_connection` — PC + negotiation deadline.
    fn create_connection(&mut self, offer_id: RtcOfferId, now: Instant) -> &mut Connection {
        self.connections.entry(offer_id).or_insert_with(|| {
            let mut pc = self.factory.create(&self.stun_server);
            let _ = &mut pc; // placeholder for onStateChange wiring (injected)
            Connection {
                pc,
                pid: None,
                incoming: false,
                deadline: now + self.connection_timeout,
            }
        })
    }

    /// `on_generated_offer`.
    fn on_generated_offer(&mut self, offer_id: RtcOfferId, sdp: String) {
        let offer = RtcOffer {
            id: offer_id,
            pid: self.pid,
            sdp,
            answer_callback: None,
        };
        while let Some(b) = self.offer_batches.front() {
            if b.is_complete() {
                self.offer_batches.pop_front();
            } else {
                break;
            }
        }
        if let Some(b) = self.offer_batches.front_mut() {
            b.add(Some(offer));
            self.flush_batches();
        }
    }

    /// `on_generated_answer` — feed `answer_callback`.
    fn on_generated_answer(&mut self, offer_id: RtcOfferId, sdp: String) {
        let Some(offer) = self.pending_offers.remove(&offer_id) else {
            return;
        };
        if !self.connections.contains_key(&offer_id) {
            return;
        }
        let answer = RtcAnswer {
            offer_id,
            pid: self.pid,
            sdp,
        };
        self.outbox
            .push_back(RtcSigOut::AnswerGenerated { offer, answer });
    }

    /// `on_data_channel` — conn lifecycle end (success or failure).
    /// On `Failed`, upstream's `onStateChange` posts *both* the
    /// description handler (`on_generated_offer(ec)` → batch `--count`)
    /// and `on_data_channel(ec)` → conn removal. Mirror both here.
    fn on_data_channel(&mut self, offer_id: RtcOfferId, dc: Option<Arc<dyn RtcChannel>>) {
        let Some(conn) = self.connections.get(&offer_id).map(|c| c.incoming) else {
            return;
        };
        if dc.is_none() && !conn {
            // negotiation failed before the channel opened —
            // `on_generated_offer(ec)` → `offer_batch::add(ec)`
            if let Some(b) = self.offer_batches.front_mut() {
                b.add(None);
                self.flush_batches();
            }
        }
        self.remove_connection(offer_id);
        let Some(dc) = dc else { return };
        self.outbox.push_back(RtcSigOut::StreamReady {
            channel: dc,
            inbound: Arc::new(Mutex::new(RtcInbound {
                queue: VecDeque::new(),
                pending: VecDeque::new(),
                closed: false,
                bad_message: false,
                waker: None,
                write_waker: None,
            })),
        });
    }

    /// Completed batches move to the outbox (`offer_batch` handler).
    fn flush_batches(&mut self) {
        while let Some(b) = self.offer_batches.front() {
            if !b.is_complete() {
                break;
            }
            if let Some(b) = self.offer_batches.pop_front() {
                self.outbox.push_back(RtcSigOut::Offers(b.offers));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Debug)]
    struct MockPc {
        remote: Mutex<Vec<String>>,
        channels: AtomicUsize,
        fail_remote: bool,
    }
    impl RtcPeerConnection for MockPc {
        fn create_data_channel(&mut self, _label: &str) {
            self.channels.fetch_add(1, Ordering::Relaxed);
        }
        fn set_remote_description(&mut self, sdp: &str, _k: SdpKind) -> Result<(), String> {
            if self.fail_remote {
                return Err("bad sdp".to_string());
            }
            self.remote.lock().unwrap().push(sdp.to_string());
            Ok(())
        }
    }

    #[derive(Debug)]
    struct MockFactory {
        created: AtomicUsize,
        fail_remote: AtomicBool,
    }
    impl RtcPeerConnectionFactory for MockFactory {
        fn create(&self, _stun: &str) -> Box<dyn RtcPeerConnection> {
            self.created.fetch_add(1, Ordering::Relaxed);
            Box::new(MockPc {
                remote: Mutex::new(Vec::new()),
                channels: AtomicUsize::new(0),
                fail_remote: self.fail_remote.load(Ordering::Relaxed),
            })
        }
    }

    #[derive(Debug)]
    struct Chan;
    impl RtcChannel for Chan {
        fn send(&self, _d: &[u8]) {}
        fn available(&self) -> usize {
            0
        }
        fn is_open(&self) -> bool {
            true
        }
        fn close(&self) {}
    }

    fn sig() -> (RtcSignaling, Arc<MockFactory>) {
        let f = Arc::new(MockFactory {
            created: AtomicUsize::new(0),
            fail_remote: AtomicBool::new(false),
        });
        (
            RtcSignaling::new(
                f.clone(),
                PeerId::from_bytes(b"-BD0001-0123456789ab").unwrap(),
                "stun:1".to_string(),
                Duration::from_secs(10),
                2,
            ),
            f,
        )
    }

    fn offers(s: &mut RtcSignaling) -> Vec<RtcOffer> {
        match s.poll_out() {
            Some(RtcSigOut::Offers(o)) => o,
            _ => panic!("expected offers"),
        }
    }

    #[test]
    fn generate_offers_batch() {
        let (mut s, f) = sig();
        let now = Instant::now();
        s.generate_offers(3, now);
        assert_eq!(f.created.load(Ordering::Relaxed), 3);
        // no output until all 3 gathering completes arrive
        assert!(s.poll_out().is_none());
        let ids: Vec<RtcOfferId> = s.connections.keys().copied().collect();
        for (i, id) in ids.iter().enumerate() {
            s.on_event(*id, RtcEvent::GatheringComplete(format!("sdp{i}")));
        }
        let got = offers(&mut s);
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].sdp, "sdp0");
    }

    #[test]
    fn zero_count_completes_immediately() {
        let (mut s, _f) = sig();
        s.generate_offers(0, Instant::now());
        assert!(offers(&mut s).is_empty());
    }

    #[test]
    fn incoming_offer_answered() {
        let (mut s, _f) = sig();
        let now = Instant::now();
        let id = [9u8; 20];
        let offer = RtcOffer {
            id,
            pid: PeerId::from_bytes(b"0123456789abcdefghij").unwrap(),
            sdp: "remote-sdp".to_string(),
            answer_callback: None,
        };
        s.process_offer(offer, now);
        assert_eq!(s.num_incoming, 1);
        s.on_event(id, RtcEvent::GatheringComplete("answer-sdp".to_string()));
        match s.poll_out() {
            Some(RtcSigOut::AnswerGenerated { answer, .. }) => {
                assert_eq!(answer.offer_id, id);
                assert_eq!(answer.sdp, "answer-sdp");
            }
            _ => panic!("expected answer"),
        }
    }

    #[test]
    fn duplicate_and_limit() {
        let (mut s, _f) = sig();
        let now = Instant::now();
        let mk = |id| RtcOffer {
            id,
            pid: PeerId::from_bytes(b"0123456789abcdefghij").unwrap(),
            sdp: "x".to_string(),
            answer_callback: None,
        };
        s.process_offer(mk([1u8; 20]), now);
        s.process_offer(mk([1u8; 20]), now); // duplicate → ignored
        assert_eq!(s.connections.len(), 1);
        s.process_offer(mk([2u8; 20]), now);
        s.process_offer(mk([3u8; 20]), now); // over max=2 → rejected
        assert_eq!(s.num_incoming, 2);
    }

    #[test]
    fn answer_dedup_and_unknown() {
        let (mut s, _f) = sig();
        let now = Instant::now();
        s.generate_offers(1, now);
        let id = *s.connections.keys().next().unwrap();
        let ans = |p| RtcAnswer {
            offer_id: id,
            pid: PeerId::from_bytes(p).unwrap(),
            sdp: "a".to_string(),
        };
        s.process_answer(&ans(b"peer0000000000000001")); // sets pid
        s.process_answer(&ans(b"peer0000000000000002")); // already answered → ignored
        assert_eq!(s.connections.len(), 1);
        s.process_answer(&RtcAnswer {
            offer_id: [7u8; 20],
            pid: PeerId::default(),
            sdp: "x".to_string(),
        }); // unknown offer → ignored
    }

    #[test]
    fn timeout_drops_connection() {
        let (mut s, _f) = sig();
        let now = Instant::now();
        s.generate_offers(1, now);
        let id = *s.connections.keys().next().unwrap();
        s.tick(now + Duration::from_secs(11));
        assert!(!s.connections.contains_key(&id));
        // upstream: timeout only erases the conn — the batch stays open
        // (only the description-handler failure path decrements it)
        assert!(s.poll_out().is_none());
    }

    #[test]
    fn failed_offer_counts_against_batch() {
        let (mut s, _f) = sig();
        let now = Instant::now();
        s.generate_offers(1, now);
        let id = *s.connections.keys().next().unwrap();
        // `onStateChange(Failed)` → description handler error + conn drop
        s.on_event(id, RtcEvent::Failed);
        assert!(!s.connections.contains_key(&id));
        assert!(offers(&mut s).is_empty());
    }

    #[test]
    fn data_channel_emits_stream() {
        let (mut s, _f) = sig();
        let now = Instant::now();
        s.generate_offers(1, now);
        let id = *s.connections.keys().next().unwrap();
        s.on_event(id, RtcEvent::DataChannel(Arc::new(Chan)));
        match s.poll_out() {
            Some(RtcSigOut::StreamReady { channel, .. }) => assert!(channel.is_open()),
            _ => panic!("expected stream"),
        }
        assert!(s.connections.is_empty());
    }
}
