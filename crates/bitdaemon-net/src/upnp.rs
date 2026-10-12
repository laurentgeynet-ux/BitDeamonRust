// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `upnp` — the SOAP core of `upnp.cpp`: port-mapping request
//! bodies (verbatim XML), device-description `controlURL`/
//! `serviceType` discovery, `GetExternalIPAddress` parsing.
//! Transport (SSDP multicast + HTTP POST to the control URL) is
//! injected — same model as `natpmp`/`DatagramSocket` (ADR-0008).
//!
//! Ported pieces: `create_port_mapping`/`delete_port_mapping`/
//! `get_ip_address` request builders, `find_control_url`, the
//! `find_ip_address` response walk, `soap_error_code` extraction.

use std::net::IpAddr;

/// `portmap_protocol`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortmapProtocol {
    /// TCP mapping.
    Tcp,
    /// UDP mapping.
    Udp,
}

impl PortmapProtocol {
    /// upstream `to_string(portmap_protocol)` → `"TCP"`/`"UDP"`.
    #[must_use]
    pub fn to_str(self) -> &'static str {
        match self {
            Self::Tcp => "TCP",
            Self::Udp => "UDP",
        }
    }
}

/// SSDP multicast endpoint (`239.255.255.250:1900`) — upstream
/// `multicast_ip`/`port` constants.
pub const SSDP_ADDR: std::net::Ipv4Addr = std::net::Ipv4Addr::new(239, 255, 255, 250);
/// SSDP port.
pub const SSDP_PORT: u16 = 1900;

/// `upnp::discover` M-SEARCH payload — search targets are the WAN
/// connection services `parse_device_description` looks for.
#[must_use]
pub fn msearch_request() -> String {
    "M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: \"ssdp:discover\"\r\nMX: 2\r\nST: urn:schemas-upnp-org:device:InternetGatewayDevice:1\r\n\r\n".to_string()
}

/// Extract the `LOCATION:` header from an SSDP reply (`HTTP/1.1 200`)
/// or a NOTIFY — `None` for non-SSDP datagrams (the UDP demux calls
/// this on every packet that isn't tracker/NAT-PMP/uTP).
#[must_use]
pub fn parse_ssdp_location(buf: &[u8]) -> Option<String> {
    let s = std::str::from_utf8(buf).ok()?;
    let head = s.split("\r\n\r\n").next()?;
    let mut lines = head.lines();
    let first = lines.next()?;
    if !first.starts_with("HTTP/1.1 200") && !first.starts_with("NOTIFY ") {
        return None;
    }
    let mut location = None;
    let mut is_igd = false;
    for l in lines {
        let Some((k, v)) = l.split_once(':') else {
            continue;
        };
        match k.trim().to_ascii_lowercase().as_str() {
            "location" => location = Some(v.trim().to_string()),
            "st" | "nt" => {
                is_igd = v.to_ascii_lowercase().contains("internetgatewaydevice");
            }
            _ => {}
        }
    }
    (location.is_some() && (is_igd || first.starts_with("HTTP"))).then_some(location?)
}

/// Resolve `control_url`/`url_base` from a parsed device description
/// against the description's fetch URL (UPnP control URLs are often
/// relative, e.g. `/ctl/IPConn`).
pub fn resolve_control_url(dev: &mut RootDevice, description_url: &str) {
    if dev.control_url.starts_with("http") {
        return;
    }
    let base = if dev.url_base.starts_with("http") {
        dev.url_base.trim_end_matches('/').to_string()
    } else {
        // scheme://authority of the description URL
        let d = description_url.trim_end_matches('/');
        match d
            .find("://")
            .and_then(|i| d[i + 3..].find('/').map(|j| i + 3 + j))
        {
            Some(end) => d[..end].to_string(),
            None => d.to_string(),
        }
    };
    if !dev.control_url.starts_with('/') && !dev.control_url.is_empty() {
        dev.control_url = format!("/{}", dev.control_url);
    }
    dev.control_url = format!("{base}{}", dev.control_url);
}

/// `rootdevice` — a discovered UPnP gateway.
#[derive(Debug, Clone, Default)]
pub struct RootDevice {
    /// the URL we POST SOAP actions to (`controlURL` resolved
    /// against `URLBase`/the description URL).
    pub control_url: String,
    /// `serviceType` of the WAN connection service.
    pub service_namespace: String,
    /// device `modelName` (informational).
    pub model: String,
    /// `URLBase` from the device description.
    pub url_base: String,
    /// external IP learned via `GetExternalIPAddress`.
    pub external_ip: Option<IpAddr>,
    /// local address used to reach the device — upstream reads it
    /// from `c.socket().local_endpoint()` on the control connection
    /// and sends it as `NewInternalClient`.
    pub local_addr: Option<IpAddr>,
}

/// `create_port_mapping` SOAP body — verbatim XML.
#[must_use]
pub fn build_add_mapping(
    service_ns: &str,
    external_port: u16,
    proto: PortmapProtocol,
    local_port: u16,
    local_addr: &str,
    description: &str,
    lease_secs: u32,
) -> String {
    format!(
        "<?xml version=\"1.0\"?>\n<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"><s:Body><u:AddPortMapping xmlns:u=\"{service_ns}\"><NewRemoteHost></NewRemoteHost><NewExternalPort>{external_port}</NewExternalPort><NewProtocol>{proto}</NewProtocol><NewInternalPort>{local_port}</NewInternalPort><NewInternalClient>{local_addr}</NewInternalClient><NewEnabled>1</NewEnabled><NewPortMappingDescription>{description}</NewPortMappingDescription><NewLeaseDuration>{lease_secs}</NewLeaseDuration></u:AddPortMapping></s:Body></s:Envelope>",
        proto = proto.to_str(),
    )
}

/// `delete_port_mapping` SOAP body — verbatim XML.
#[must_use]
pub fn build_delete_mapping(
    service_ns: &str,
    external_port: u16,
    proto: PortmapProtocol,
) -> String {
    format!(
        "<?xml version=\"1.0\"?>\n<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"><s:Body><u:DeletePortMapping xmlns:u=\"{service_ns}\"><NewRemoteHost></NewRemoteHost><NewExternalPort>{external_port}</NewExternalPort><NewProtocol>{proto}</NewProtocol></u:DeletePortMapping></s:Body></s:Envelope>",
        proto = proto.to_str(),
    )
}

/// `get_ip_address` SOAP body — verbatim XML.
#[must_use]
pub fn build_get_ip(service_ns: &str) -> String {
    format!(
        "<?xml version=\"1.0\"?>\n<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"><s:Body><u:GetExternalIPAddress xmlns:u=\"{service_ns}\"></u:GetExternalIPAddress></s:Body></s:Envelope>"
    )
}

/// `SOAPAction` header value — `"service#action"`.
#[must_use]
pub fn soap_action(service_ns: &str, action: &str) -> String {
    format!("\"{service_ns}#{action}\"")
}

/// minimal SAX-ish tag walk (upstream `xml_parse` is also a
/// non-validating scanner). Yields `(is_close, tag_lower, text)`.
fn scan_xml(body: &str, mut on_text: impl FnMut(&[String], &str)) {
    let mut stack: Vec<String> = Vec::new();
    let bytes = body.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'<' {
            if bytes[i + 1..].starts_with(b"!--") {
                if let Some(end) = body[i..].find("-->") {
                    i += end + 3;
                    continue;
                }
                break;
            }
            let end = match body[i..].find('>') {
                Some(e) => i + e,
                None => break,
            };
            let tag = body[i + 1..end].trim_end_matches('/').trim();
            if let Some(close) = tag.strip_prefix('/') {
                if stack
                    .last()
                    .is_some_and(|t| t.eq_ignore_ascii_case(close.trim()))
                {
                    stack.pop();
                }
            } else if !tag.starts_with('?') && !tag.starts_with('!') {
                let name = tag.split_whitespace().next().unwrap_or("");
                stack.push(name.to_ascii_lowercase());
            }
            i = end + 1;
        } else {
            let end = body[i..].find('<').map_or(body.len(), |e| i + e);
            let text = body[i..end].trim();
            if !text.is_empty() {
                on_text(&stack, text);
            }
            i = end;
        }
    }
}

/// `find_control_url` — extract `controlURL`, `serviceType`,
/// `modelName`, `URLBase` from a device-description document.
/// `service_type` is only kept for the WAN connection services
/// upstream supports.
#[must_use]
pub fn parse_device_description(xml: &str) -> RootDevice {
    const WANTED: &[&str] = &[
        "urn:schemas-upnp-org:service:wanipconnection:1",
        "urn:schemas-upnp-org:service:wanipconnection:2",
        "urn:schemas-upnp-org:service:wanpppconnection:1",
    ];
    let mut dev = RootDevice::default();
    // set by the current <service>'s <serviceType> text — mirrors
    // upstream `state.in_service` (first matching service wins).
    let mut in_service = false;
    scan_xml(xml, |stack, text| {
        if stack.iter().rev().nth(1).is_some_and(|t| t == "service")
            && stack.last().is_some_and(|t| t == "servicetype")
        {
            in_service = dev.service_namespace.is_empty()
                && WANTED.contains(&text.to_ascii_lowercase().as_str());
            if in_service {
                dev.service_namespace = text.to_string();
            }
            return;
        }
        if dev.control_url.is_empty()
            && in_service
            && stack.iter().rev().nth(1).is_some_and(|t| t == "service")
            && stack.last().is_some_and(|t| t == "controlurl")
        {
            dev.control_url = text.to_string();
        } else if dev.model.is_empty()
            && stack.iter().rev().nth(1).is_some_and(|t| t == "device")
            && stack.last().is_some_and(|t| t == "modelname")
        {
            dev.model = text.to_string();
        } else if stack.last().is_some_and(|t| t == "urlbase") {
            dev.url_base = text.to_string();
        }
    });
    dev
}

/// `find_ip_address` — pull `NewExternalIPAddress` (or a SOAP fault
/// `errorCode`) out of a `GetExternalIPAddress` response.
pub fn parse_external_ip(xml: &str) -> Result<IpAddr, i32> {
    let mut ip = None;
    let mut error = -1i32;
    scan_xml(xml, |stack, text| {
        if stack.last().is_some_and(|t| t == "newexternalipaddress") {
            ip = text.parse().ok();
        } else if stack.last().is_some_and(|t| t == "errorcode") {
            error = text.parse().unwrap_or(-1);
        }
    });
    ip.ok_or(error)
}

/// `find_error_code` — extract a SOAP fault `errorCode` from any
/// response (`soap_error_code` upstream).
#[must_use]
pub fn soap_error_code(xml: &str) -> i32 {
    let mut error = -1i32;
    scan_xml(xml, |stack, text| {
        if stack.last().is_some_and(|t| t == "errorcode") {
            error = text.parse().unwrap_or(-1);
        }
    });
    error
}

/// `mapping_t` — per-device mapping state (`portmap_action`).
#[derive(Debug, Clone)]
pub struct UpnpMapping {
    /// `portmap_action`.
    pub act: MappingAction,
    /// `portmap_protocol::none` = unmapped.
    pub protocol: Option<PortmapProtocol>,
    /// external (router-side) port; randomized on conflicts.
    pub external_port: u16,
    /// our listening port.
    pub local_port: u16,
    /// our local address (NewInternalClient).
    pub local_addr: String,
    /// `m.failcount` — retries before giving up (upstream: 5).
    pub failcount: u32,
    /// `m.expires` (renewal due) — epoch secs, 0 = no lease.
    pub expires: u64,
}

/// `portmap_action`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MappingAction {
    /// nothing pending.
    None,
    /// create the mapping.
    Add,
    /// delete the mapping.
    Delete,
}

/// what the caller must do next (upstream performs `post()` /
/// `on_port_mapping()` inline).
#[derive(Debug)]
pub enum UpnpOut {
    /// POST `body` to `control_url` with `SOAPAction: action`.
    Post {
        /// mapping slot index.
        mapping: usize,
        /// SOAPAction value.
        action: String,
        /// SOAP body.
        body: String,
    },
    /// mapping confirmed → `on_port_mapping` callback.
    Mapped {
        /// mapping slot index.
        mapping: usize,
        /// external IP reported by the router.
        external_ip: Option<IpAddr>,
        /// external port in use.
        external_port: u16,
        /// protocol mapped.
        proto: PortmapProtocol,
    },
    /// mapping failed — `return_error` upstream.
    MapError {
        /// mapping slot index.
        mapping: usize,
        /// UPnP `errorCode`.
        code: i32,
    },
}

/// `upnp` per-device driver — the `update_map`/`next`/`on_upnp_map_response`
/// state machine, minus io_service.
#[derive(Debug)]
pub struct UpnpDriver {
    /// the gateway.
    pub dev: RootDevice,
    /// `d.mapping` — one slot per global mapping.
    pub mappings: Vec<UpnpMapping>,
    /// `d.use_lease_duration` — routers that reject leases.
    pub use_lease_duration: bool,
    /// `settings_pack::upnp_lease_duration` (0 = permanent).
    pub lease_duration: u32,
    /// `mapping_desc` — NewPortMappingDescription.
    pub description: String,
}

/// `mapping_t` ctor state.
impl UpnpDriver {
    /// a discovered device (`control_url`/`service_namespace` filled).
    #[must_use]
    pub fn new(dev: RootDevice, lease_duration: u32, description: String) -> Self {
        Self {
            dev,
            mappings: Vec::new(),
            use_lease_duration: true,
            lease_duration,
            description,
        }
    }

    /// `upnp::set_mappings` — sync the per-device table with the
    /// session's mapping list; returns actions for slots needing
    /// work (upstream calls `update_map` on each).
    pub fn set_mappings(&mut self, maps: Vec<(PortmapProtocol, u16, u16, String)>) -> Vec<UpnpOut> {
        self.mappings.resize(
            maps.len(),
            UpnpMapping {
                act: MappingAction::None,
                protocol: None,
                external_port: 0,
                local_port: 0,
                local_addr: String::new(),
                failcount: 0,
                expires: 0,
            },
        );
        let mut out = Vec::new();
        for (i, (proto, ext, local, addr)) in maps.into_iter().enumerate() {
            let m = &mut self.mappings[i];
            m.act = MappingAction::Add;
            m.protocol = Some(proto);
            m.external_port = ext;
            m.local_port = local;
            m.local_addr = addr;
            if !self.dev.service_namespace.is_empty() {
                out.extend(self.update_map(i));
            }
        }
        out
    }

    /// `add_port_mapping` — append one mapping slot (upstream's
    /// `upnp::set_mapping` on a new `mapping_t` index). The
    /// `AddPortMapping` SOAP call goes out via `update_map`.
    pub fn add_mapping(
        &mut self,
        proto: PortmapProtocol,
        external_port: u16,
        local_port: u16,
        local_addr: String,
    ) -> Vec<UpnpOut> {
        self.mappings.push(UpnpMapping {
            act: MappingAction::Add,
            protocol: Some(proto),
            external_port,
            local_port,
            local_addr,
            failcount: 0,
            expires: 0,
        });
        let i = self.mappings.len() - 1;
        if self.dev.service_namespace.is_empty() {
            Vec::new()
        } else {
            self.update_map(i)
        }
    }

    /// `delete_mapping` — upstream marks `act=del` then `update_map`.
    pub fn delete_mapping(&mut self, i: usize) -> Vec<UpnpOut> {
        if i >= self.mappings.len() {
            return Vec::new();
        }
        let m = &mut self.mappings[i];
        m.act = MappingAction::Delete;
        self.update_map(i)
    }

    /// `update_map` — next SOAP call for slot `i`, or none when the
    /// slot needs no work / retries exhausted (`failcount > 5`).
    pub fn update_map(&mut self, i: usize) -> Vec<UpnpOut> {
        let Some(m) = self.mappings.get(i).cloned() else {
            return Vec::new();
        };
        match m.act {
            MappingAction::Add if m.protocol.is_some() => {
                if m.failcount > 5 {
                    self.mappings[i].act = MappingAction::None;
                    return self.next(i);
                }
                let lease = if self.use_lease_duration {
                    self.lease_duration
                } else {
                    0
                };
                vec![UpnpOut::Post {
                    mapping: i,
                    action: soap_action(&self.dev.service_namespace, "AddPortMapping"),
                    body: build_add_mapping(
                        &self.dev.service_namespace,
                        m.external_port,
                        m.protocol.unwrap_or(PortmapProtocol::Tcp),
                        m.local_port,
                        &m.local_addr,
                        &self.description,
                        lease,
                    ),
                }]
            }
            MappingAction::Delete if m.protocol.is_some() => vec![UpnpOut::Post {
                mapping: i,
                action: soap_action(&self.dev.service_namespace, "DeletePortMapping"),
                body: build_delete_mapping(
                    &self.dev.service_namespace,
                    m.external_port,
                    m.protocol.unwrap_or(PortmapProtocol::Tcp),
                ),
            }],
            _ => self.next(i),
        }
    }

    /// `next` — advance to the following slot.
    pub fn next(&mut self, i: usize) -> Vec<UpnpOut> {
        if i + 1 < self.mappings.len() {
            self.update_map(i + 1)
        } else {
            Vec::new()
        }
    }

    /// `on_upnp_xml` finished → issue `GetExternalIPAddress`.
    #[must_use]
    pub fn discover(&self) -> UpnpOut {
        UpnpOut::Post {
            mapping: 0,
            action: soap_action(&self.dev.service_namespace, "GetExternalIPAddress"),
            body: build_get_ip(&self.dev.service_namespace),
        }
    }

    /// `on_get_ip_address` — store the external IP, then start the
    /// first mapping.
    pub fn on_ip_response(&mut self, body: &str) -> Vec<UpnpOut> {
        if let Ok(ip) = parse_external_ip(body) {
            self.dev.external_ip = Some(ip);
        }
        let mut out = Vec::new();
        if !self.mappings.is_empty() {
            out.extend(self.update_map(0));
        }
        out
    }

    /// `on_upnp_map_response` — handle a SOAP reply for slot `i`.
    /// `body` = HTTP response body (already validated XML).
    /// Error codes verbatim: 725 permanent-leases-only → retry w/o
    /// lease; 718/501/716 → external port conflict → random port;
    /// other → `return_error`.
    pub fn on_map_response(&mut self, i: usize, body: &str, rand_port: u16) -> Vec<UpnpOut> {
        let code = soap_error_code(body);
        let m = &mut self.mappings[i];
        match code {
            725 => {
                self.use_lease_duration = false;
                m.act = MappingAction::Add;
                m.failcount += 1;
                self.update_map(i)
            }
            718 | 501 | 716 if m.failcount < 4 => {
                m.external_port = 40000 + (rand_port % 10000);
                m.act = MappingAction::Add;
                m.failcount += 1;
                self.update_map(i)
            }
            -1 => {
                if m.act == MappingAction::Delete {
                    m.protocol = None;
                    m.act = MappingAction::None;
                    return self.next(i);
                }
                let out = UpnpOut::Mapped {
                    mapping: i,
                    external_ip: self.dev.external_ip,
                    external_port: m.external_port,
                    proto: m.protocol.unwrap_or(PortmapProtocol::Tcp),
                };
                let mut v = vec![out];
                if self.use_lease_duration && self.lease_duration != 0 {
                    self.mappings[i].expires = now_secs() + u64::from(self.lease_duration) / 2;
                } else {
                    self.mappings[i].expires = 0;
                }
                self.mappings[i].act = MappingAction::None;
                v.extend(self.next(i));
                v
            }
            c => {
                if c == 727 {
                    m.act = MappingAction::None;
                }
                let mut v = vec![UpnpOut::MapError {
                    mapping: i,
                    code: c,
                }];
                v.extend(self.next(i));
                v
            }
        }
    }

    /// `map_timer` expiry — `use_lease_duration` mappings renew at
    /// half the lease (upstream re-adds when `expires` passes).
    pub fn renew_due(&mut self, i: usize, now: u64) -> Vec<UpnpOut> {
        let m = &self.mappings[i];
        if m.act == MappingAction::None
            && m.protocol.is_some()
            && m.expires != 0
            && now >= m.expires
        {
            self.mappings[i].act = MappingAction::Add;
            return self.update_map(i);
        }
        Vec::new()
    }
}

/// `seconds` clock for leases.
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
