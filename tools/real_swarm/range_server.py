#!/usr/bin/env python3
"""Minimal HTTP file server with Range (206) support for web-seed tests."""
import os, re, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = sys.argv[1] if len(sys.argv) > 1 else "."
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 8901

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split("?")[0].lstrip("/") or "index.html"
        fp = os.path.normpath(os.path.join(ROOT, path))
        if not fp.startswith(os.path.abspath(ROOT)) and not fp.startswith(os.path.abspath(".") + os.sep):
            fp = os.path.join(os.path.abspath(ROOT), os.path.basename(fp))
        if not os.path.isfile(fp):
            self.send_error(404); return
        size = os.path.getsize(fp)
        rng = self.headers.get("Range")
        m = re.match(r"bytes=(\d+)-(\d*)", rng or "")
        if m:
            start = int(m.group(1))
            end = int(m.group(2)) if m.group(2) else size - 1
            end = min(end, size - 1)
            if start > end:
                self.send_error(416); return
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.send_header("Content-Length", str(end - start + 1))
            self.end_headers()
            with open(fp, "rb") as f:
                f.seek(start); self.wfile.write(f.read(end - start + 1))
        else:
            self.send_response(200)
            self.send_header("Content-Length", str(size))
            self.end_headers()
            with open(fp, "rb") as f:
                self.wfile.write(f.read())
    def log_message(self, *a):
        sys.stderr.write("%s %s\n" % (self.address_string(), a[0] % a[1:]))

ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
