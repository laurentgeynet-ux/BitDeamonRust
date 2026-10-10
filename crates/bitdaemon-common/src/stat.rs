//! `aux::stat_channel` + `aux::stat` — per-second rate counters with a
//! 5-second sliding average (`stat.hpp`, `stat.cpp`).
//!
//! Channels and accounting formulas (IP overhead MTU math, SYN costs) are
//! transcribed from upstream — session-stats consumers depend on them.

/// `aux::stat_channel` — one counter + total + sliding rate estimate.
#[derive(Debug, Clone, Default)]
pub struct StatChannel {
    /// `m_total_counter` — all-time total.
    total_counter: i64,
    /// `m_counter` — accumulator for the current tick interval.
    counter: i32,
    /// `m_5_sec_average`.
    average: i32,
}

impl StatChannel {
    /// `add(count)`.
    pub fn add(&mut self, count: i32) {
        debug_assert!(count >= 0);
        self.counter += count;
        self.total_counter += i64::from(count);
    }

    /// `second_tick(tick_interval_ms)` — call once per tick (nominally 1 s).
    /// `sample = counter * 1000 / ms`; `avg = avg*4/5 + sample/5`.
    pub fn second_tick(&mut self, tick_interval_ms: i32) {
        let sample = i64::from(self.counter) * 1000 / i64::from(tick_interval_ms);
        debug_assert!(sample >= 0);
        self.average = (i64::from(self.average) * 4 / 5 + sample / 5) as i32;
        self.counter = 0;
    }

    /// `rate()` — the 5-second sliding average.
    #[must_use]
    pub fn rate(&self) -> i32 {
        self.average
    }

    /// `low_pass_rate()` — same estimator upstream.
    #[must_use]
    pub fn low_pass_rate(&self) -> i32 {
        self.average
    }

    /// `total()`.
    #[must_use]
    pub fn total(&self) -> i64 {
        self.total_counter
    }

    /// `offset(c)` — add to the total without touching the rate window
    /// (used when a peer carries stats over from a previous connection).
    pub fn offset(&mut self, c: i64) {
        self.total_counter += c;
    }

    /// `counter()` — current tick's accumulator.
    #[must_use]
    pub fn counter(&self) -> i32 {
        self.counter
    }

    /// `clear()`.
    pub fn clear(&mut self) {
        self.counter = 0;
        self.average = 0;
        self.total_counter = 0;
    }
}

impl std::ops::AddAssign<&Self> for StatChannel {
    /// `operator+=` — merge another channel's counters.
    fn add_assign(&mut self, s: &Self) {
        self.counter += s.counter;
        self.total_counter += i64::from(s.counter);
    }
}

/// Channel indices — `aux::stat`'s anonymous enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum StatChannelKind {
    /// `upload_payload`.
    UploadPayload = 0,
    /// `upload_protocol`.
    UploadProtocol = 1,
    /// `download_payload`.
    DownloadPayload = 2,
    /// `download_protocol`.
    DownloadProtocol = 3,
    /// `upload_ip_protocol` — IP/TCP header overhead, sent.
    UploadIpProtocol = 4,
    /// `download_ip_protocol` — IP/TCP header overhead, received.
    DownloadIpProtocol = 5,
}

/// `stat::num_channels`.
pub const NUM_CHANNELS: usize = 6;

/// `aux::stat` — the six transfer channels.
#[derive(Debug, Clone, Default)]
pub struct Stat {
    stat: [StatChannel; NUM_CHANNELS],
}

impl Stat {
    /// `sent_syn(ipv6)` — a TCP SYN costs 60 bytes (v6) or 40 (v4) overhead.
    pub fn sent_syn(&mut self, ipv6: bool) {
        self.stat[StatChannelKind::UploadIpProtocol as usize].add(if ipv6 { 60 } else { 40 });
    }

    /// `received_synack(ipv6)` — SYN-ACK in, ACK out.
    pub fn received_synack(&mut self, ipv6: bool) {
        let sz = if ipv6 { 60 } else { 40 };
        self.stat[StatChannelKind::DownloadIpProtocol as usize].add(sz);
        self.stat[StatChannelKind::UploadIpProtocol as usize].add(sz);
    }

    /// `received_bytes(payload, protocol)`.
    pub fn received_bytes(&mut self, bytes_payload: i32, bytes_protocol: i32) {
        self.stat[StatChannelKind::DownloadPayload as usize].add(bytes_payload);
        self.stat[StatChannelKind::DownloadProtocol as usize].add(bytes_protocol);
    }

    /// `sent_bytes(payload, protocol)`.
    pub fn sent_bytes(&mut self, bytes_payload: i32, bytes_protocol: i32) {
        self.stat[StatChannelKind::UploadPayload as usize].add(bytes_payload);
        self.stat[StatChannelKind::UploadProtocol as usize].add(bytes_protocol);
    }

    /// `trancieve_ip_packet(bytes, ipv6)` — account TCP/IP overhead for a
    /// packet in both directions (each packet implies an ACK).
    pub fn trancieve_ip_packet(&mut self, bytes_transferred: i32, ipv6: bool) {
        // IPv4 header 20 B / IPv6 40 B + TCP 20 B
        let header = (if ipv6 { 40 } else { 20 }) + 20;
        let mtu = 1500;
        let packet_size = mtu - header;
        // upstream: max(1, ceil(bytes / packet_size)) * header
        let overhead = (bytes_transferred + packet_size - 1)
            .div_euclid(packet_size)
            .max(1)
            * header;
        self.stat[StatChannelKind::DownloadIpProtocol as usize].add(overhead);
        self.stat[StatChannelKind::UploadIpProtocol as usize].add(overhead);
    }

    /// `second_tick(ms)` on all channels.
    pub fn second_tick(&mut self, tick_interval_ms: i32) {
        for c in &mut self.stat {
            c.second_tick(tick_interval_ms);
        }
    }

    /// `upload_ip_overhead()` — current-tick IP overhead sent.
    #[must_use]
    pub fn upload_ip_overhead(&self) -> i32 {
        self.stat[StatChannelKind::UploadIpProtocol as usize].counter()
    }

    /// `download_ip_overhead()` — current-tick IP overhead received.
    #[must_use]
    pub fn download_ip_overhead(&self) -> i32 {
        self.stat[StatChannelKind::DownloadIpProtocol as usize].counter()
    }

    /// `low_pass_upload_rate()`.
    #[must_use]
    pub fn low_pass_upload_rate(&self) -> i32 {
        self.rate_sum(&[
            StatChannelKind::UploadPayload,
            StatChannelKind::UploadProtocol,
            StatChannelKind::UploadIpProtocol,
        ])
    }

    /// `low_pass_download_rate()`.
    #[must_use]
    pub fn low_pass_download_rate(&self) -> i32 {
        self.rate_sum(&[
            StatChannelKind::DownloadPayload,
            StatChannelKind::DownloadProtocol,
            StatChannelKind::DownloadIpProtocol,
        ])
    }

    /// `upload_rate()`.
    #[must_use]
    pub fn upload_rate(&self) -> i32 {
        self.low_pass_upload_rate()
    }

    /// `download_rate()`.
    #[must_use]
    pub fn download_rate(&self) -> i32 {
        self.low_pass_download_rate()
    }

    /// `total_upload()`.
    #[must_use]
    pub fn total_upload(&self) -> i64 {
        self.total_sum(&[
            StatChannelKind::UploadPayload,
            StatChannelKind::UploadProtocol,
            StatChannelKind::UploadIpProtocol,
        ])
    }

    /// `total_download()`.
    #[must_use]
    pub fn total_download(&self) -> i64 {
        self.total_sum(&[
            StatChannelKind::DownloadPayload,
            StatChannelKind::DownloadProtocol,
            StatChannelKind::DownloadIpProtocol,
        ])
    }

    /// `upload_payload_rate()`.
    #[must_use]
    pub fn upload_payload_rate(&self) -> i32 {
        self.stat[StatChannelKind::UploadPayload as usize].rate()
    }

    /// `download_payload_rate()`.
    #[must_use]
    pub fn download_payload_rate(&self) -> i32 {
        self.stat[StatChannelKind::DownloadPayload as usize].rate()
    }

    /// `total_payload_upload()`.
    #[must_use]
    pub fn total_payload_upload(&self) -> i64 {
        self.stat[StatChannelKind::UploadPayload as usize].total()
    }

    /// `total_payload_download()`.
    #[must_use]
    pub fn total_payload_download(&self) -> i64 {
        self.stat[StatChannelKind::DownloadPayload as usize].total()
    }

    /// `total_protocol_upload()`.
    #[must_use]
    pub fn total_protocol_upload(&self) -> i64 {
        self.stat[StatChannelKind::UploadProtocol as usize].total()
    }

    /// `total_protocol_download()`.
    #[must_use]
    pub fn total_protocol_download(&self) -> i64 {
        self.stat[StatChannelKind::DownloadProtocol as usize].total()
    }

    /// `total_transfer(channel)`.
    #[must_use]
    pub fn total_transfer(&self, channel: StatChannelKind) -> i64 {
        self.stat[channel as usize].total()
    }

    /// `transfer_rate(channel)`.
    #[must_use]
    pub fn transfer_rate(&self, channel: StatChannelKind) -> i32 {
        self.stat[channel as usize].rate()
    }

    /// `add_stat(downloaded, uploaded)` — seed payload totals (peer carry-over).
    pub fn add_stat(&mut self, downloaded: i64, uploaded: i64) {
        self.stat[StatChannelKind::DownloadPayload as usize].offset(downloaded);
        self.stat[StatChannelKind::UploadPayload as usize].offset(uploaded);
    }

    /// `last_payload_downloaded()`.
    #[must_use]
    pub fn last_payload_downloaded(&self) -> i32 {
        self.stat[StatChannelKind::DownloadPayload as usize].counter()
    }

    /// `last_payload_uploaded()`.
    #[must_use]
    pub fn last_payload_uploaded(&self) -> i32 {
        self.stat[StatChannelKind::UploadPayload as usize].counter()
    }

    /// `last_protocol_downloaded()`.
    #[must_use]
    pub fn last_protocol_downloaded(&self) -> i32 {
        self.stat[StatChannelKind::DownloadProtocol as usize].counter()
    }

    /// `last_protocol_uploaded()`.
    #[must_use]
    pub fn last_protocol_uploaded(&self) -> i32 {
        self.stat[StatChannelKind::UploadProtocol as usize].counter()
    }

    /// `operator[]` — raw channel access.
    #[must_use]
    pub fn channel(&self, i: StatChannelKind) -> &StatChannel {
        &self.stat[i as usize]
    }

    /// `clear()`.
    pub fn clear(&mut self) {
        for c in &mut self.stat {
            c.clear();
        }
    }

    /// `operator+=`.
    pub fn add_assign_stat(&mut self, s: &Self) {
        for (a, b) in self.stat.iter_mut().zip(s.stat.iter()) {
            *a += b;
        }
    }

    fn rate_sum(&self, kinds: &[StatChannelKind]) -> i32 {
        kinds.iter().map(|&k| self.stat[k as usize].rate()).sum()
    }

    fn total_sum(&self, kinds: &[StatChannelKind]) -> i64 {
        kinds.iter().map(|&k| self.stat[k as usize].total()).sum()
    }
}
