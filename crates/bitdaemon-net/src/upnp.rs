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
