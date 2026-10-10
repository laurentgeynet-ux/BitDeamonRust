// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! `upnp` — SOAP builders verbatim + device-description parsing.

use bitdaemon_net::upnp::{
    PortmapProtocol, build_add_mapping, build_delete_mapping, build_get_ip,
    parse_device_description, parse_external_ip, soap_action, soap_error_code,
};

const ROOTDESC: &str = r#"<?xml version="1.0"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
  <URLBase>http://192.168.1.1:80/</URLBase>
  <device>
    <modelName>Netgear R7000</modelName>
    <serviceList>
      <service>
        <serviceType>urn:schemas-upnp-org:service:Layer3Forwarding:1</serviceType>
        <controlURL>/ctl/L3F</controlURL>
      </service>
      <service>
        <serviceType>urn:schemas-upnp-org:service:WANIPConnection:1</serviceType>
        <controlURL>/ctl/IPConn</controlURL>
      </service>
    </serviceList>
  </device>
</root>"#;

#[test]
fn parses_wanip_connection_service() {
    let dev = parse_device_description(ROOTDESC);
    assert_eq!(
        dev.service_namespace,
        "urn:schemas-upnp-org:service:WANIPConnection:1"
    );
    assert_eq!(dev.control_url, "/ctl/IPConn");
    assert_eq!(dev.url_base, "http://192.168.1.1:80/");
    assert_eq!(dev.model, "Netgear R7000");
}

#[test]
fn add_mapping_body_verbatim() {
    let body = build_add_mapping(
        "urn:schemas-upnp-org:service:WANIPConnection:1",
        6881,
        PortmapProtocol::Tcp,
        6881,
        "10.0.0.2",
        "bitdaemon",
        7200,
    );
    assert!(
        body.contains(
            "<u:AddPortMapping xmlns:u=\"urn:schemas-upnp-org:service:WANIPConnection:1\">"
        )
    );
    assert!(body.contains("<NewExternalPort>6881</NewExternalPort>"));
    assert!(body.contains("<NewProtocol>TCP</NewProtocol>"));
    assert!(body.contains("<NewInternalClient>10.0.0.2</NewInternalClient>"));
    assert!(body.contains("<NewLeaseDuration>7200</NewLeaseDuration>"));
}

#[test]
fn delete_and_getip_bodies() {
    let del = build_delete_mapping("ns:wan", 6881, PortmapProtocol::Udp);
    assert!(del.contains("<u:DeletePortMapping xmlns:u=\"ns:wan\">"));
    assert!(del.contains("<NewProtocol>UDP</NewProtocol>"));
    assert!(!del.contains("NewInternalClient"));
    let get = build_get_ip("ns:wan");
    assert!(get.contains("<u:GetExternalIPAddress xmlns:u=\"ns:wan\">"));
    assert_eq!(
        soap_action("ns:wan", "AddPortMapping"),
        "\"ns:wan#AddPortMapping\""
    );
}

#[test]
fn external_ip_and_fault_parsing() {
    let ok = "<s:Envelope><s:Body><u:GetExternalIPAddressResponse>\
              <NewExternalIPAddress>192.168.160.19</NewExternalIPAddress>\
              </u:GetExternalIPAddressResponse></s:Body></s:Envelope>";
    assert_eq!(parse_external_ip(ok).unwrap().to_string(), "192.168.160.19");
    let fault = "<s:Envelope><s:Body><s:Fault><detail><UPnPError>\
                 <errorCode>606</errorCode></UPnPError></detail>\
                 </s:Fault></s:Body></s:Envelope>";
    assert_eq!(parse_external_ip(fault), Err(606));
    assert_eq!(soap_error_code(fault), 606);
}
