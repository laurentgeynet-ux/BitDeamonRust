//! Port of the BEP-10 extended-handshake half of
//! `bt_peer_connection.cpp` (`write_extensions` +
//! `on_extended_handshake`): builds and parses the bencoded extended
//! handshake, including the `[len][0x14][0x00]` framing.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use bitdaemon_bencode::{Entry, bdecode, bencode};

/// Built-in extension message ids (upstream `upload_only_msg` & co.).
/// Ids 1-4 are libtorrent-local conventions used in `m`.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExtendedIds {
    /// `upload_only` message id.
    pub upload_only: u8,
    /// `ut_holepunch` message id.
    pub holepunch: u8,
    /// `share_mode` message id.
    pub share_mode: u8,
    /// `lt_donthave` message id.
    pub dont_have: u8,
}

/// The fields `write_extensions` puts in the handshake.
#[derive(Debug, Clone, Default)]
pub struct ExtendedHandshake {
    /// `m` — extension name → message id (BEP-10).
    pub messages: BTreeMap<String, i64>,
    /// `p` — our listen port (sent only on outgoing connections).
    pub listen_port: Option<i64>,
    /// `v` — client version string (omitted in anonymous mode).
    pub client_version: Option<Vec<u8>>,
    /// `yourip` — the remote's IP as seen by us (4 or 16 bytes).
    pub your_ip: Option<Vec<u8>>,
    /// `reqq` — max allowed incoming request queue.
    pub reqq: i64,
    /// `complete_ago` — seconds since we last saw a complete copy
    /// (-1 = never).
    pub complete_ago: i64,
    /// `upload_only` — we only want to upload.
    pub upload_only: bool,
    /// `share_mode`.
    pub share_mode: bool,
    /// `metadata_size` — info dict size for `ut_metadata`.
    pub metadata_size: Option<i64>,
    /// `ipv4`/`ipv6` compact hints.
    pub ipv4: Option<Ipv4Addr>,
    /// `ipv6`.
    pub ipv6: Option<Ipv6Addr>,
}

impl ExtendedHandshake {
    /// `write_extensions` — serialize to the bencoded handshake dict.
    #[must_use]
    pub fn build(&self) -> Entry {
        let mut h = BTreeMap::new();
        let m: BTreeMap<Vec<u8>, Entry> = self
            .messages
            .iter()
            .map(|(k, v)| (k.clone().into_bytes(), Entry::Int(*v)))
            .collect();
        h.insert(b"m".to_vec(), Entry::Dict(m));
        if let Some(p) = self.listen_port {
            h.insert(b"p".to_vec(), Entry::Int(p));
        }
        if let Some(v) = &self.client_version {
            h.insert(b"v".to_vec(), Entry::String(v.clone()));
        }
        if let Some(ip) = &self.your_ip {
            h.insert(b"yourip".to_vec(), Entry::String(ip.clone()));
        }
        h.insert(b"reqq".to_vec(), Entry::Int(self.reqq));
        h.insert(b"complete_ago".to_vec(), Entry::Int(self.complete_ago));
        if self.upload_only {
            h.insert(b"upload_only".to_vec(), Entry::Int(1));
        }
        if self.share_mode {
            h.insert(b"share_mode".to_vec(), Entry::Int(1));
        }
        if let Some(ms) = self.metadata_size {
            h.insert(b"metadata_size".to_vec(), Entry::Int(ms));
        }
        if let Some(v4) = self.ipv4 {
            h.insert(b"ipv4".to_vec(), Entry::String(v4.octets().to_vec()));
        }
        if let Some(v6) = self.ipv6 {
            h.insert(b"ipv6".to_vec(), Entry::String(v6.octets().to_vec()));
        }
        Entry::Dict(h)
    }

    /// Encode with the wire framing `[len u32][0x14][0x00]` + bencode.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let dict_msg = bencode(&self.build());
        let mut out = Vec::with_capacity(dict_msg.len() + 6);
        out.extend(((dict_msg.len() + 2) as u32).to_be_bytes());
        out.push(0x14); // msg_extended
        out.push(0); // handshake sub-message
        out.extend(dict_msg);
        out
    }

    /// `on_extended_handshake` — parse the handshake body (the bytes
    /// *after* the `[0x14][0x00]` header).
    pub fn parse(body: &[u8]) -> Option<Self> {
        let root = bdecode(body);
        if root.node_type() != bitdaemon_bencode::NodeType::Dict {
            return None;
        }
        let mut hs = Self::default();
        let m = root.dict_find_dict(b"m");
        if m.node_type() == bitdaemon_bencode::NodeType::Dict {
            let mut msgs = BTreeMap::new();
            for i in 0..m.dict_size() {
                let (key, node) = m.dict_at(i);
                if node.node_type() == bitdaemon_bencode::NodeType::Int {
                    msgs.insert(String::from_utf8_lossy(key).into_owned(), node.int_value());
                }
            }
            hs.messages = msgs;
        }
        let p = root.dict_find_int_value(b"p", 0);
        if p > 0 {
            hs.listen_port = Some(p);
        }
        let v = root.dict_find_string_value(b"v", &[]);
        if !v.is_empty() {
            hs.client_version = Some(v.to_vec());
        }
        let yourip = root.dict_find_string_value(b"yourip", &[]);
        if !yourip.is_empty() {
            hs.your_ip = Some(yourip.to_vec());
        }
        hs.reqq = root.dict_find_int_value(b"reqq", 0);
        hs.complete_ago = root.dict_find_int_value(b"complete_ago", -1);
        hs.upload_only = root.dict_find_int_value(b"upload_only", 0) != 0;
        hs.share_mode = root.dict_find_int_value(b"share_mode", 0) != 0;
        let ms = root.dict_find_int_value(b"metadata_size", 0);
        if ms > 0 {
            hs.metadata_size = Some(ms);
        }
        Some(hs)
    }

    /// `on_extended_handshake` side-effects — the extension ids the
    /// peer registered.
    #[must_use]
    pub fn remote_ids(&self) -> ExtendedIds {
        ExtendedIds {
            upload_only: self.messages.get("upload_only").copied().unwrap_or(0) as u8,
            holepunch: self.messages.get("ut_holepunch").copied().unwrap_or(0) as u8,
            share_mode: self.messages.get("share_mode").copied().unwrap_or(0) as u8,
            dont_have: self.messages.get("lt_donthave").copied().unwrap_or(0) as u8,
        }
    }

    /// `your_ip` decoded as an `IpAddr` (4 or 16 bytes).
    #[must_use]
    pub fn remote_ip(&self) -> Option<IpAddr> {
        match self.your_ip.as_deref()? {
            [a, b, c, d] => Some(IpAddr::V4(Ipv4Addr::new(*a, *b, *c, *d))),
            v if v.len() == 16 => {
                let mut o = [0u8; 16];
                o.copy_from_slice(v);
                Some(IpAddr::V6(Ipv6Addr::from(o)))
            }
            _ => None,
        }
    }
}
