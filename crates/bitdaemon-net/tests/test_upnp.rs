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

fn ok_reply(body: &str) -> bitdaemon_net::upnp::SoapReply<'_> {
    bitdaemon_net::upnp::SoapReply::Full {
        status: 200,
        content_type: Some("text/xml"),
        body,
    }
}

fn dev() -> bitdaemon_net::upnp::RootDevice {
    bitdaemon_net::upnp::RootDevice {
        control_url: "http://192.168.0.1:49152/ctl/IPConn".into(),
        service_namespace: "urn:schemas-upnp-org:service:WANIPConnection:1".into(),
        hostname: "192.168.0.1".into(),
        port: 49152,
        path: "/ctl/IPConn".into(),
        ..Default::default()
    }
}

#[test]
fn msearch_verbatim_upstream() {
    // the exact byte stream `discover_device_impl` sends
    assert_eq!(
        bitdaemon_net::upnp::msearch_request(),
        "M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nST:upnp:rootdevice\r\nMAN:\"ssdp:discover\"\r\nMX:3\r\n\r\n\r\n"
    );
}

#[test]
fn ssdp_location_accepts_notify_and_200() {
    // upstream takes any `200` reply or `NOTIFY` with a location —
    // no ST/NT filtering (non-routers die at the WAN-service check)
    let notify = b"NOTIFY * HTTP/1.1\r\nHost: 239.255.255.250:1900\r\nNT:urn:x\r\nLocation: http://10.0.0.1:2869/d\r\n\r\n";
    assert_eq!(
        bitdaemon_net::upnp::parse_ssdp_location(notify).as_deref(),
        Some("http://10.0.0.1:2869/d")
    );
    let ok = b"HTTP/1.1 200 OK\r\nLocation: http://192.168.1.1:5431/d\r\n\r\n";
    assert_eq!(
        bitdaemon_net::upnp::parse_ssdp_location(ok).as_deref(),
        Some("http://192.168.1.1:5431/d")
    );
    let err = b"HTTP/1.1 500 X\r\nLocation: http://x/y\r\n\r\n";
    assert_eq!(bitdaemon_net::upnp::parse_ssdp_location(err), None);
    let no_loc = b"HTTP/1.1 200 OK\r\n\r\n";
    assert_eq!(bitdaemon_net::upnp::parse_ssdp_location(no_loc), None);
}

#[test]
fn invalid_upnp_host_and_location_gate() {
    use bitdaemon_net::upnp::{invalid_upnp_host, validate_location};
    assert!(invalid_upnp_host("0.0.0.0"));
    assert!(invalid_upnp_host("127.0.0.1"));
    assert!(invalid_upnp_host("169.254.1.1"));
    assert!(invalid_upnp_host("ff02::1"));
    assert!(!invalid_upnp_host("192.168.1.1"));
    assert!(!invalid_upnp_host("router.local"));
    // http only, port 0 rejected, missing port → 80
    assert!(validate_location("https://192.168.1.1/x").is_none());
    assert!(validate_location("http://192.168.1.1:0/x").is_none());
    assert!(validate_location("http://127.0.0.1/x").is_none());
    assert_eq!(
        validate_location("http://192.168.1.1/d").map(|(h, p, _)| (h, p)),
        Some(("192.168.1.1".into(), 80))
    );
}

#[test]
fn resolve_control_url_variants() {
    use bitdaemon_net::upnp::{RootDevice, resolve_control_url};
    // url_base + relative control url
    let mut d = RootDevice {
        url_base: "http://192.168.1.1:80/".into(),
        control_url: "/ctl/IPConn".into(),
        service_namespace: "urn:schemas-upnp-org:service:WANIPConnection:1".into(),
        ..Default::default()
    };
    assert!(resolve_control_url(
        &mut d,
        "http://192.168.1.1:80/root.xml"
    ));
    assert_eq!(d.control_url, "http://192.168.1.1:80/ctl/IPConn");
    assert_eq!(d.hostname, "192.168.1.1");
    assert_eq!(d.port, 80);
    assert_eq!(d.path, "/ctl/IPConn");

    // bare /path, no URLBase → description host:port prepended
    let mut d = RootDevice {
        control_url: "/ctl".into(),
        service_namespace: "x".into(),
        ..Default::default()
    };
    assert!(resolve_control_url(
        &mut d,
        "http://192.168.1.2:5431/root.xml"
    ));
    assert_eq!(d.control_url, "http://192.168.1.2:5431/ctl");

    // empty control url / invalid host → device dropped
    let mut d = RootDevice::default();
    assert!(!resolve_control_url(&mut d, "http://192.168.1.1/r"));
    let mut d = RootDevice {
        control_url: "http://127.0.0.1/ctl".into(),
        ..Default::default()
    };
    assert!(!resolve_control_url(&mut d, "http://192.168.1.1/r"));
}

#[test]
fn driver_map_flow_and_retries() {
    use bitdaemon_net::upnp::{SoapReply, UpnpDriver, UpnpOut};
    let mut d = UpnpDriver::new(dev(), 3600, "bitdaemon".into());
    let out = d.set_mappings(vec![(PortmapProtocol::Tcp, 6881, 6881, "10.0.0.2".into())]);
    assert_eq!(out.len(), 1);
    let req = match &out[0] {
        UpnpOut::Post { request, .. } => request.clone(),
        _ => panic!("expected Post"),
    };
    assert!(req.contains("AddPortMapping"));
    // upstream `post()` writes verbatim: Soapaction w/ lowercase s,
    // charset, no Connection header
    assert!(req.starts_with("POST /ctl/IPConn HTTP/1.1\r\nHost: 192.168.0.1:49152\r\n"));
    assert!(req.contains("Content-Type: text/xml; charset=\"utf-8\"\r\n"));
    assert!(
        req.contains(
            "Soapaction: \"urn:schemas-upnp-org:service:WANIPConnection:1#AddPortMapping\""
        )
    );
    // no `Connection` header upstream (the namespace string itself
    // contains "Connection:" — match an actual header line)
    assert!(!req.contains("\r\nConnection:"));
    // in-flight watchdog state
    assert!(d.pending);

    // second mapping queues behind the in-flight request
    let outs = d
        .add_mapping(PortmapProtocol::Udp, 6881, 6881, "10.0.0.2".into())
        .map(|(_, o)| o)
        .unwrap_or_default();
    assert!(outs.is_empty());
    assert!(d.mappings[1].act == bitdaemon_net::upnp::MappingAction::Add);

    // 718 conflict → random port retry
    let fault718 = "<s><s:Fault><errorCode>718</errorCode></s:Fault></s>";
    let out = d.on_map_response(0, ok_reply(fault718), 12345);
    match &out[0] {
        UpnpOut::Post { request, .. } => {
            assert!(request.contains("<NewExternalPort>42345</NewExternalPort>"))
        }
        _ => panic!("expected retry Post"),
    }

    // success → Mapped + lease armed at lease*3/4, then the queued
    // UDP mapping posts via the `next` sweep
    let ok = "<s><s:Body><u:AddPortMappingResponse/></s:Body></s>";
    let out = d.on_map_response(0, ok_reply(ok), 0);
    assert!(matches!(
        out[0],
        UpnpOut::Mapped {
            external_port: 42345,
            ..
        }
    ));
    assert!(out.iter().any(|o| matches!(
        o,
        UpnpOut::Post {
            kind: bitdaemon_net::upnp::SoapKind::Add,
            ..
        }
    )));
    assert!(d.mappings[0].expires > 0);
    assert_eq!(d.mappings[0].failcount, 0);

    // finish the UDP add
    let out = d.on_map_response(1, ok_reply(ok), 0);
    assert!(matches!(out[0], UpnpOut::Mapped { mapping: 1, .. }));

    // renew due → re-add
    let out = d.renew_due(0, u64::MAX);
    assert!(matches!(out[0], UpnpOut::Post { .. }));

    // delete flow — the reply feeds `on_unmap_response`, not the
    // map handler
    let out = d.on_map_response(0, ok_reply(ok), 0);
    assert!(matches!(out[0], UpnpOut::Mapped { .. }));
    let out = d.delete_mapping(0);
    assert!(matches!(
        out[0],
        UpnpOut::Post {
            kind: bitdaemon_net::upnp::SoapKind::Delete,
            ..
        }
    ));
    let out = d.on_unmap_response(0, ok_reply(ok));
    assert!(matches!(out[0], UpnpOut::Unmapped { mapping: 0, .. }));
    assert!(d.mappings[0].protocol.is_none());

    // transport error on a map request disables the device
    let out = d.on_map_response(1, SoapReply::TransportError, 0);
    assert!(out.is_empty());
    assert!(d.dev.disabled);
}

#[test]
fn content_type_gate_and_wrap_sweep() {
    use bitdaemon_net::upnp::{UpnpDriver, UpnpOut};
    let mut d = UpnpDriver::new(dev(), 3600, "bitdaemon".into());
    let _ = d.set_mappings(vec![(PortmapProtocol::Tcp, 6881, 6881, "10.0.0.2".into())]);
    // non-XML content-type on the in-flight add → next() (no retry,
    // no disable)
    let out = d.on_map_response(
        0,
        bitdaemon_net::upnp::SoapReply::Full {
            status: 200,
            content_type: Some("text/html"),
            body: "junk",
        },
        0,
    );
    assert!(out.is_empty());
    assert!(!d.dev.disabled);

    // upstream `next` sweeps `d.mapping` for `act != none` once the
    // last slot completes — a delete queued on an *earlier* slot is
    // picked up when the tail of the list finishes
    let ok = "<s><s:Body><u:AddPortMappingResponse/></s:Body></s>";
    let _ = d.add_mapping(PortmapProtocol::Udp, 6882, 6882, "10.0.0.2".into());
    let _ = d.add_mapping(PortmapProtocol::Tcp, 6883, 6883, "10.0.0.2".into());
    // slot 1's Add is in flight; slot 2 queued behind it
    let out = d.on_map_response(1, ok_reply(ok), 0);
    assert!(out.iter().any(|o| matches!(
        o,
        UpnpOut::Post {
            kind: bitdaemon_net::upnp::SoapKind::Add,
            mapping: 2,
            ..
        }
    )));
    // slot 2 in flight → mark slot 0 for deletion (stays queued)
    assert!(d.delete_mapping(0).is_empty());
    // last slot's reply → `next(2)` → sweep finds slot 0 (act=del)
    let out = d.on_map_response(2, ok_reply(ok), 0);
    assert!(out.iter().any(|o| matches!(
        o,
        UpnpOut::Post {
            kind: bitdaemon_net::upnp::SoapKind::Delete,
            mapping: 0,
            ..
        }
    )));
}
