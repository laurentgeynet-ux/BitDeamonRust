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

/// `upnp::discover_device_impl` M-SEARCH payload — byte-exact
/// (upstream searches `upnp:rootdevice`, not a specific service).
#[must_use]
pub fn msearch_request() -> String {
    "M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nST:upnp:rootdevice\r\nMAN:\"ssdp:discover\"\r\nMX:3\r\n\r\n\r\n".to_string()
}

/// Extract the `LOCATION:` header from an SSDP reply (`HTTP/1.1 200`)
/// or a NOTIFY — `None` for anything else. Upstream accepts *any*
/// 200/`notify` datagram carrying a location header (it does not
/// filter on `ST:`/`NT:` — non-routers are rejected later by
/// `invalid_upnp_host` and the WAN-service check in
/// [`parse_device_description`]).
#[must_use]
pub fn parse_ssdp_location(buf: &[u8]) -> Option<String> {
    let s = std::str::from_utf8(buf).ok()?;
    let head = s.split("\r\n\r\n").next()?;
    let mut lines = head.lines();
    let first = lines.next()?;
    // upstream: `p.status_code() != 200 && p.method() != "notify"`
    let status_ok = first
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse::<i64>().ok())
        == Some(200);
    let is_notify = first
        .split_whitespace()
        .next()
        .is_some_and(|m| m.eq_ignore_ascii_case("notify"));
    if !status_ok && !is_notify {
        return None;
    }
    for l in lines {
        if let Some((k, v)) = l.split_once(':')
            && k.trim().eq_ignore_ascii_case("location")
        {
            let v = v.trim();
            return (!v.is_empty()).then(|| v.to_string());
        }
    }
    None
}

/// `invalid_upnp_host` — reject LOCATION/control-URL IP literals a
/// real IGD would never use (unspecified, loopback, link-local,
/// multicast); DNS names pass through.
#[must_use]
pub fn invalid_upnp_host(hostname: &str) -> bool {
    match hostname.parse::<IpAddr>() {
        Ok(a) => {
            a.is_unspecified()
                || a.is_loopback()
                || crate::ip_helpers::is_link_local(&a)
                || a.is_multicast()
        }
        Err(_) => false,
    }
}

/// the LOCATION URL check in `on_reply` — `http` scheme only, port
/// default 80, port 0 rejected, host must pass `invalid_upnp_host`.
/// Returns `(host, port, path)`.
#[must_use]
pub fn validate_location(url: &str) -> Option<(String, u16, String)> {
    let c = crate::parse_url::parse_url_components(url).ok()?;
    let mut port = c.port;
    if port == -1 {
        port = if c.protocol == "http" { 80 } else { 443 };
    }
    if c.protocol != "http" {
        return None;
    }
    if port == 0 || invalid_upnp_host(&c.hostname) {
        return None;
    }
    let path = if c.path.is_empty() {
        "/".to_string()
    } else {
        c.path.clone()
    };
    Some((c.hostname, u16::try_from(port).ok()?, path))
}

/// Resolve `control_url` from a parsed device description
/// (`on_upnp_xml`'s tail): `url_base + control_url` when the control
/// URL is not `http://`-absolute (a trailing `/` on `url_base` is
/// stripped only when the control URL itself starts with `/`); a
/// bare `/path` gets `scheme://host:port` of the *description* URL
/// prepended (port defaulting to 80/443 by scheme). Then parses
/// `control_url` into `dev.hostname`/`dev.port`/`dev.path`.
///
/// Returns `false` when the device must be dropped — `control_url`
/// empty, unparseable, or `invalid_upnp_host` (upstream marks
/// `d.disabled` in all these cases).
#[must_use]
pub fn resolve_control_url(dev: &mut RootDevice, description_url: &str) -> bool {
    if dev.control_url.is_empty() {
        return false;
    }
    if !dev.url_base.is_empty() && !dev.control_url.starts_with("http://") {
        // avoid double slashes in path
        if dev.url_base.ends_with('/') && dev.control_url.starts_with('/') {
            dev.url_base.pop();
        }
        dev.control_url = format!("{}{}", dev.url_base, dev.control_url);
    }
    if dev.control_url.starts_with('/') {
        let Ok(c) = crate::parse_url::parse_url_components(description_url) else {
            return false;
        };
        let port = if c.port == -1 {
            if c.protocol == "http" { 80 } else { 443 }
        } else {
            c.port
        };
        dev.control_url = format!(
            "{}://{}:{}{}",
            c.protocol, c.hostname, port, dev.control_url
        );
    }
    let Ok(c) = crate::parse_url::parse_url_components(&dev.control_url) else {
        return false;
    };
    dev.port = if c.port == -1 {
        if c.protocol == "http" { 80 } else { 443 }
    } else {
        c.port
    };
    dev.hostname = c.hostname;
    dev.path = c.path;
    !invalid_upnp_host(&dev.hostname)
}

/// `rootdevice` — a discovered UPnP gateway.
#[derive(Debug, Clone, Default)]
pub struct RootDevice {
    /// `d.url` — the LOCATION we got the description from.
    pub url: String,
    /// the URL we POST SOAP actions to (`controlURL` resolved
    /// against `URLBase`/the description URL).
    pub control_url: String,
    /// `serviceType` of the WAN connection service.
    pub service_namespace: String,
    /// device `modelName` (informational).
    pub model: String,
    /// `URLBase` from the device description.
    pub url_base: String,
    /// `d.hostname` — host part of `control_url` (filled by
    /// [`resolve_control_url`]).
    pub hostname: String,
    /// `d.port` — port of `control_url` (80/443 default).
    pub port: i32,
    /// `d.path` — path of `control_url` (the POST target).
    pub path: String,
    /// `d.disabled` — set when the description fetch, control URL
    /// or a mapping request fails; disables every mapping on the
    /// device.
    pub disabled: bool,
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

/// which SOAP call a [`UpnpOut::Post`] represents — upstream binds
/// the reply handler at connection-creation time
/// (`on_upnp_map_response` vs `on_upnp_unmap_response` vs
/// `on_upnp_get_ip_address_response`); the caller routes replies
/// the same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SoapKind {
    /// `AddPortMapping` → `on_upnp_map_response`.
    Add,
    /// `DeletePortMapping` → `on_upnp_unmap_response`.
    Delete,
    /// `GetExternalIPAddress` → `on_upnp_get_ip_address_response`.
    GetIp,
}

/// a completed HTTP exchange for an in-flight SOAP request — the
/// pieces of `(error_code, aux::http_parser)` the upstream handlers
/// inspect.
#[derive(Debug, Clone, Copy)]
pub enum SoapReply<'a> {
    /// `e && e != eof` — connect/send/recv failed.
    TransportError,
    /// `!p.header_finished()` — reply never completed headers.
    Incomplete,
    /// a full reply (status may be ≠ 200 — upstream still parses the
    /// body for `errorCode` on map/unmap replies).
    Full {
        /// `p.status_code()`.
        status: i32,
        /// `p.header("content-type")`.
        content_type: Option<&'a str>,
        /// `p.get_body()`.
        body: &'a str,
    },
}

/// what the caller must do next (upstream performs `post()` /
/// `on_port_mapping()` inline).
#[derive(Debug)]
pub enum UpnpOut {
    /// send `request` verbatim to the device's control endpoint
    /// (`{hostname}:{port}`) — it is the exact byte stream
    /// `upnp::post()` writes to the socket.
    Post {
        /// mapping slot index.
        mapping: usize,
        /// which reply handler this request feeds.
        kind: SoapKind,
        /// `POST … HTTP/1.1` + headers + SOAP body, verbatim.
        request: String,
    },
    /// mapping confirmed → `on_port_mapping` success callback.
    Mapped {
        /// mapping slot index.
        mapping: usize,
        /// external IP reported by the router (empty upstream).
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
    /// `DeletePortMapping` finished — `on_upnp_unmap_response`'s
    /// `on_port_mapping` callback. `code` is the HTTP status for
    /// non-200 replies (transport errors → -1), else the SOAP
    /// `errorCode` — which is -1 even on a clean delete (upstream
    /// quirk: it reports `error_code(-1, upnp_category())`).
    Unmapped {
        /// mapping slot index.
        mapping: usize,
        /// protocol that was unmapped.
        proto: PortmapProtocol,
        /// error indication, see variant docs.
        code: i32,
    },
}

/// `aux::is_upnp_xml_content_type` — `text/xml`, `text/soap+xml`,
/// `application/xml`, `application/soap+xml` (parameters stripped,
/// case-insensitive).
#[must_use]
pub fn is_upnp_xml_content_type(content_type: &str) -> bool {
    let ct = content_type.split(';').next().unwrap_or("").trim();
    ct.eq_ignore_ascii_case("text/xml")
        || ct.eq_ignore_ascii_case("text/soap+xml")
        || ct.eq_ignore_ascii_case("application/xml")
        || ct.eq_ignore_ascii_case("application/soap+xml")
}

/// byte-exact SOAP POST (`post()` upstream): `POST path HTTP/1.1` +
/// Host (via `format_host_for_connect` — brackets for v6
/// literals)/`Content-Type: text/xml; charset="utf-8"`/
/// Content-Length/`Soapaction: "ns#action"` (lowercase s —
/// verbatim). No `Connection` header — upstream sends exactly this.
#[must_use]
pub fn build_post(
    hostname: &str,
    port: i32,
    path: &str,
    service_ns: &str,
    action: &str,
    body: &str,
) -> String {
    format!(
        "POST {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: text/xml; charset=\"utf-8\"\r\nContent-Length: {}\r\nSoapaction: \"{service_ns}#{action}\"\r\n\r\n{body}",
        crate::string_util::format_host_for_connect(
            hostname.to_string(),
            u16::try_from(port).unwrap_or(0),
        ),
        body.len(),
    )
}

/// `max_global_mappings` upstream — the global slot table (which
/// `d.mapping` mirrors per-device) never exceeds 50.
pub const MAX_MAPPINGS: usize = 50;

/// `upnp` per-device driver — the `update_map`/`next`/
/// `on_upnp_*_response` state machine, minus io_service. One
/// outstanding HTTP request per device (`d.upnp_connection`
/// upstream → [`UpnpDriver::pending`]).
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
    /// `mapping_desc` — NewPortMappingDescription (upstream sends
    /// `user_agent`, or "" in anonymous mode — pass accordingly).
    pub description: String,
    /// a SOAP request is in flight (`d.upnp_connection != nullptr`
    /// upstream); set when a `Post` is emitted, cleared by the
    /// `on_*_response` handlers.
    pub pending: bool,
}

impl UpnpDriver {
    /// a discovered device (`control_url`/`service_namespace`/
    /// `hostname`/`port`/`path` filled by [`resolve_control_url`]).
    #[must_use]
    pub fn new(dev: RootDevice, lease_duration: u32, description: String) -> Self {
        Self {
            dev,
            mappings: Vec::new(),
            use_lease_duration: true,
            lease_duration,
            description,
            pending: false,
        }
    }

    /// `num_mappings()` — global slots still in use.
    #[must_use]
    pub fn num_mappings(&self) -> usize {
        self.mappings
            .iter()
            .filter(|m| m.protocol.is_some())
            .count()
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
            if self.dev.disabled {
                break;
            }
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

    /// `upnp::add_mapping` — claim the first free slot (or append
    /// one, capped at [`MAX_MAPPINGS`]), then `update_map` it.
    /// `None` when the table is full or the device is disabled.
    pub fn add_mapping(
        &mut self,
        proto: PortmapProtocol,
        external_port: u16,
        local_port: u16,
        local_addr: String,
    ) -> Option<(usize, Vec<UpnpOut>)> {
        if self.dev.disabled {
            return None;
        }
        let i = match self.mappings.iter().position(|m| m.protocol.is_none()) {
            Some(i) => i,
            None => {
                if self.mappings.len() >= MAX_MAPPINGS {
                    return None;
                }
                self.mappings.push(UpnpMapping {
                    act: MappingAction::None,
                    protocol: None,
                    external_port: 0,
                    local_port: 0,
                    local_addr: String::new(),
                    failcount: 0,
                    expires: 0,
                });
                self.mappings.len() - 1
            }
        };
        {
            let m = &mut self.mappings[i];
            m.act = MappingAction::Add;
            m.protocol = Some(proto);
            m.external_port = external_port;
            m.local_port = local_port;
            m.local_addr = local_addr;
        }
        let out = if self.dev.service_namespace.is_empty() {
            Vec::new()
        } else {
            self.update_map(i)
        };
        Some((i, out))
    }

    /// `upnp::close` — queue `DeletePortMapping` for every mapped
    /// slot (pending adds are dropped *without* a delete, verbatim),
    /// then kick slot 0. Devices that never yielded a `control_url`
    /// are skipped upstream (`upnp.cpp` close loop). The caller
    /// still owns slot retirement.
    pub fn close(&mut self) -> Vec<UpnpOut> {
        if self.dev.control_url.is_empty() {
            for m in &mut self.mappings {
                m.act = MappingAction::None;
                m.protocol = None;
            }
            return Vec::new();
        }
        for m in &mut self.mappings {
            if m.protocol.is_none() {
                continue;
            }
            if m.act == MappingAction::Add {
                m.act = MappingAction::None;
                continue;
            }
            m.act = MappingAction::Delete;
        }
        if self.num_mappings() > 0 {
            self.update_map(0)
        } else {
            Vec::new()
        }
    }

    /// `delete_mapping` — upstream marks `act=del` then
    /// `update_map`; a slot that was never mapped is left alone
    /// (upstream early-outs on `protocol == none`).
    pub fn delete_mapping(&mut self, i: usize) -> Vec<UpnpOut> {
        if self.dev.disabled || self.mappings.get(i).is_none_or(|m| m.protocol.is_none()) {
            return Vec::new();
        }
        self.mappings[i].act = MappingAction::Delete;
        self.update_map(i)
    }

    /// `update_map` — next SOAP call for slot `i`, or none when the
    /// slot needs no work / retries exhausted (`failcount > 5`) /
    /// a request is already in flight (`d.upnp_connection` upstream).
    pub fn update_map(&mut self, i: usize) -> Vec<UpnpOut> {
        if self.pending || self.dev.disabled {
            return Vec::new();
        }
        let Some(m) = self.mappings.get(i).cloned() else {
            return Vec::new();
        };
        if m.act == MappingAction::None || m.protocol.is_none() {
            self.mappings[i].act = MappingAction::None;
            return self.next(i);
        }
        let out = match m.act {
            MappingAction::Add => {
                if m.failcount > 5 {
                    // giving up
                    self.mappings[i].act = MappingAction::None;
                    return self.next(i);
                }
                let lease = if self.use_lease_duration {
                    self.lease_duration
                } else {
                    0
                };
                UpnpOut::Post {
                    mapping: i,
                    kind: SoapKind::Add,
                    request: build_post(
                        &self.dev.hostname,
                        self.dev.port,
                        &self.dev.path,
                        &self.dev.service_namespace,
                        "AddPortMapping",
                        &build_add_mapping(
                            &self.dev.service_namespace,
                            m.external_port,
                            m.protocol.unwrap_or(PortmapProtocol::Tcp),
                            m.local_port,
                            &m.local_addr,
                            &self.description,
                            lease,
                        ),
                    ),
                }
            }
            MappingAction::Delete => UpnpOut::Post {
                mapping: i,
                kind: SoapKind::Delete,
                request: build_post(
                    &self.dev.hostname,
                    self.dev.port,
                    &self.dev.path,
                    &self.dev.service_namespace,
                    "DeletePortMapping",
                    &build_delete_mapping(
                        &self.dev.service_namespace,
                        m.external_port,
                        m.protocol.unwrap_or(PortmapProtocol::Tcp),
                    ),
                ),
            },
            MappingAction::None => unreachable!("handled above"),
        };
        // upstream: `m.act = none; m.expires = now + 30s` — the
        // in-flight watchdog; a reply clears `pending`
        self.mappings[i].act = MappingAction::None;
        self.mappings[i].expires = now_secs() + 30;
        self.pending = true;
        vec![out]
    }

    /// `next` — advance to the following slot; after the last one,
    /// upstream sweeps `d.mapping` for the first slot still flagged
    /// `act != none` (queued deletes/late adds get picked up there).
    pub fn next(&mut self, i: usize) -> Vec<UpnpOut> {
        if i + 1 < self.mappings.len() {
            return self.update_map(i + 1);
        }
        match self
            .mappings
            .iter()
            .position(|m| m.act != MappingAction::None)
        {
            Some(j) => self.update_map(j),
            None => Vec::new(),
        }
    }

    /// `on_upnp_xml` done → issue `GetExternalIPAddress` (sets
    /// `pending` — the mapping sweep waits for it like upstream's
    /// shared `d.upnp_connection`).
    pub fn discover(&mut self) -> UpnpOut {
        self.pending = true;
        UpnpOut::Post {
            mapping: 0,
            kind: SoapKind::GetIp,
            request: build_post(
                &self.dev.hostname,
                self.dev.port,
                &self.dev.path,
                &self.dev.service_namespace,
                "GetExternalIPAddress",
                &build_get_ip(&self.dev.service_namespace),
            ),
        }
    }

    /// `on_upnp_get_ip_address_response` — `None` covers every
    /// upstream failure branch (transport error, incomplete reply,
    /// status ≠ 200): they all just proceed to `update_map(0)`
    /// without an external IP. Clears `pending`.
    pub fn on_ip_response(&mut self, body: Option<&str>) -> Vec<UpnpOut> {
        self.pending = false;
        if let Some(body) = body
            && let Ok(ip) = parse_external_ip(body)
        {
            self.dev.external_ip = Some(ip);
        }
        if self.num_mappings() > 0 {
            self.update_map(0)
        } else {
            Vec::new()
        }
    }

    /// `on_upnp_map_response` — handle an `AddPortMapping` reply for
    /// slot `i`; clears `pending`. Verbatim error branches:
    /// transport error → `d.disabled`; incomplete reply or non-XML
    /// content-type → `next`; 725 → drop leases + retry;
    /// 718/501 (with `failcount < 4`) → random external port retry;
    /// any other `errorCode` (incl. 727) → `return_error`;
    /// success → `Mapped` + lease renewal at `now + lease*3/4`.
    /// `rand_port` supplies upstream's `40000 + random(10000)`.
    pub fn on_map_response(
        &mut self,
        i: usize,
        reply: SoapReply<'_>,
        rand_port: u16,
    ) -> Vec<UpnpOut> {
        self.pending = false;
        let body = match reply {
            SoapReply::TransportError => {
                self.dev.disabled = true;
                return Vec::new();
            }
            SoapReply::Incomplete => return self.next(i),
            SoapReply::Full {
                content_type, body, ..
            } => {
                if content_type.is_some_and(|ct| !ct.is_empty())
                    && !is_upnp_xml_content_type(content_type.unwrap_or(""))
                {
                    return self.next(i);
                }
                body
            }
        };
        let code = soap_error_code(body);
        let mut out = Vec::new();
        match code {
            725 => {
                // "OnlyPermanentLeasesSupported" — retry w/o lease
                self.use_lease_duration = false;
                self.mappings[i].act = MappingAction::Add;
                self.mappings[i].failcount += 1;
                return self.update_map(i);
            }
            718 | 501 if self.mappings[i].failcount < 4 => {
                // external-port conflict — retry with a random port
                self.mappings[i].external_port = 40000 + rand_port % 10000;
                self.mappings[i].act = MappingAction::Add;
                self.mappings[i].failcount += 1;
                return self.update_map(i);
            }
            -1 => {
                out.push(UpnpOut::Mapped {
                    mapping: i,
                    external_ip: self.dev.external_ip,
                    external_port: self.mappings[i].external_port,
                    proto: self.mappings[i].protocol.unwrap_or(PortmapProtocol::Tcp),
                });
                if self.use_lease_duration && self.lease_duration != 0 {
                    self.mappings[i].expires = now_secs() + u64::from(self.lease_duration) * 3 / 4;
                } else {
                    // `expires = max_time()` upstream — 0 = never
                    self.mappings[i].expires = 0;
                }
                self.mappings[i].failcount = 0;
            }
            c => {
                // `return_error` — incl. 727
                out.push(UpnpOut::MapError {
                    mapping: i,
                    code: c,
                });
            }
        }
        out.extend(self.next(i));
        out
    }

    /// `on_upnp_unmap_response` — a `DeletePortMapping` reply always
    /// fires the callback (errors included: `code` is the HTTP
    /// status when it isn't 200 — transport failures report -1 —
    /// else the SOAP `errorCode`, -1 on a clean delete), clears the
    /// slot's protocol, then `next`. Clears `pending`.
    pub fn on_unmap_response(&mut self, i: usize, reply: SoapReply<'_>) -> Vec<UpnpOut> {
        self.pending = false;
        let code = match reply {
            SoapReply::TransportError | SoapReply::Incomplete => -1,
            SoapReply::Full { status, body, .. } => {
                if status != 200 {
                    status
                } else {
                    soap_error_code(body)
                }
            }
        };
        let proto = self
            .mappings
            .get(i)
            .and_then(|m| m.protocol)
            .unwrap_or(PortmapProtocol::Tcp);
        if let Some(m) = self.mappings.get_mut(i) {
            m.protocol = None;
        }
        let mut out = vec![UpnpOut::Unmapped {
            mapping: i,
            proto,
            code,
        }];
        out.extend(self.next(i));
        out
    }

    /// `on_expire` — a slot whose `expires` passed gets re-added
    /// (`act = add; update_map`). `expires == 0` is upstream's
    /// `max_time()` sentinel (no lease). The caller sweeps all
    /// slots like upstream's per-timer loop.
    pub fn renew_due(&mut self, i: usize, now: u64) -> Vec<UpnpOut> {
        if self.dev.disabled {
            return Vec::new();
        }
        let m = &self.mappings[i];
        if m.expires != 0 && now >= m.expires {
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
