#!/usr/bin/env python3
# mini_tracker.py — minimal BitTorrent tracker for differential tests.
# HTTP announce (BEP-3) on one port, UDP announce (BEP-15) on another.
# Returns compact IPv4 peer lists; logs every request to stdout.
import socket, socketserver, struct, sys, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qsl

PEERS = {}          # info_hash -> {peer_id: (ip, port)}
LOCK = threading.Lock()

def benc(x):
    if isinstance(x, int): return b'i' + str(x).encode() + b'e'
    if isinstance(x, bytes): return str(len(x)).encode() + b':' + x
    if isinstance(x, str): return str(len(x)).encode() + b':' + x.encode()
    if isinstance(x, dict):
        out = b'd'
        for k in sorted(x, key=lambda k: k if isinstance(k, bytes) else k.encode()):
            kb = k if isinstance(k, bytes) else k.encode()
            out += str(len(kb)).encode() + b':' + kb + benc(x[k])
        return out + b'e'
    raise TypeError(x)

def peers_blob(ih, exclude_pid):
    with LOCK:
        blob = b''
        for pid, (ip, port) in PEERS.get(ih, {}).items():
            if pid == exclude_pid: continue
            try: blob += socket.inet_aton(ip) + struct.pack('>H', port)
            except OSError: pass
    return blob

def record(ih, pid, ip, port, event):
    with LOCK:
        d = PEERS.setdefault(ih, {})
        if event in ('stopped', 3): d.pop(pid, None)
        elif port: d[pid] = (ip, port)
        print(f'[tracker] {ih.hex()[:12]} {event} {ip}:{port} peers={len(d)}', flush=True)

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        u = urlparse(self.path)
        q = dict(parse_qsl(u.query, keep_blank_values=True,
                           encoding='latin-1', errors='strict'))
        ih_raw = q.get('info_hash', '')
        ih = ih_raw.encode('latin-1') if isinstance(ih_raw, str) else ih_raw
        pid = q.get('peer_id', '').encode('latin-1')
        port = int(q.get('port', '0') or 0)
        event = q.get('event', '')
        ip = self.client_address[0]
        if u.path.endswith('/scrape'):
            d = {b'files': {ih: {b'complete': 1, b'incomplete': 0, b'downloaded': 1}}}
            body = benc(d)
        else:
            record(ih, pid, ip, port, event or 'interval')
            body = benc({b'interval': 60, b'complete': 1,
                         b'incomplete': 0, b'peers': peers_blob(ih, pid)})
        self.send_response(200)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass

class U(socketserver.BaseRequestHandler):
    def handle(self):
        data, sock = self.request
        ip = self.client_address[0]
        if len(data) < 16: return
        conn_id, action, txid = struct.unpack('>QII', data[:16])
        if action == 0:                     # connect
            sock.sendto(struct.pack('>IIQ', 0, txid, 0x41727101980), self.client_address)
            return
        if action == 1 and len(data) >= 98: # announce
            (ih, pid, _dl, _left, _ul, ev, _key, _nw, _ip, port) = \
                struct.unpack('>20s20sQQQIIIiH', data[16:16+82])
            record(ih, pid, ip, port, ev)
            with LOCK:
                plist = [(i, p) for _pid, (i, p) in PEERS.get(ih, {}).items() if _pid != pid]
            body = struct.pack('>IIIII', 1, txid, 60, 0, len(plist))
            for i, p in plist:
                body += socket.inet_aton(i) + struct.pack('>H', p)
            sock.sendto(body, self.client_address)
            return
        if action == 2:                     # scrape
            ih = data[16:36]
            body = struct.pack('>II', 2, txid) + struct.pack('>III', 1, 1, 0)
            sock.sendto(body, self.client_address)

def main():
    http_port, udp_port = int(sys.argv[1]), int(sys.argv[2])
    hs = ThreadingHTTPServer(('127.0.0.1', http_port), H)
    threading.Thread(target=hs.serve_forever, daemon=True).start()
    us = socketserver.UDPServer(('127.0.0.1', udp_port), U)
    print(f'[tracker] http :{http_port} udp :{udp_port}', flush=True)
    us.serve_forever()

if __name__ == '__main__':
    main()
