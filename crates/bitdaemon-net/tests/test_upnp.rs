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

#[test]
fn driver_map_flow_and_retries() {
    use bitdaemon_net::upnp::{RootDevice, UpnpDriver, UpnpOut};
    let dev = RootDevice {
        control_url: "/ctl/IPConn".into(),
        service_namespace: "urn:schemas-upnp-org:service:WANIPConnection:1".into(),
        ..Default::default()
    };
    let mut d = UpnpDriver::new(dev, 3600, "bitdaemon".into());
    let out = d.set_mappings(vec![(PortmapProtocol::Tcp, 6881, 6881, "10.0.0.2".into())]);
    assert_eq!(out.len(), 1);
    let body = match &out[0] {
        UpnpOut::Post { body, .. } => body.clone(),
        _ => panic!("expected Post"),
    };
    assert!(body.contains("AddPortMapping"));

    // 718 conflict → random port retry
    let fault718 = "<s><s:Fault><errorCode>718</errorCode></s:Fault></s>";
    let out = d.on_map_response(0, fault718, 12345);
    match &out[0] {
        UpnpOut::Post { body, .. } => {
            assert!(body.contains("<NewExternalPort>42345</NewExternalPort>"))
        }
        _ => panic!("expected retry Post"),
    }

    // success → Mapped + lease renew armed
    let ok = "<s><s:Body><u:AddPortMappingResponse/></s:Body></s>";
    let out = d.on_map_response(0, ok, 0);
    assert!(matches!(
        out[0],
        UpnpOut::Mapped {
            external_port: 42345,
            ..
        }
    ));
    assert!(d.mappings[0].expires > 0);

    // renew due → re-add
    let out = d.renew_due(0, u64::MAX);
    assert!(matches!(out[0], UpnpOut::Post { .. }));

    // delete flow
    let out = d.delete_mapping(0);
    assert!(matches!(out[0], UpnpOut::Post { .. }));
    let out = d.on_map_response(0, ok, 0);
    assert!(out.is_empty());
    assert!(d.mappings[0].protocol.is_none());
}
