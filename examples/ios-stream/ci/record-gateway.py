#!/usr/bin/env python3
"""A gateway stand-in that keeps every playlist the publisher writes (#228).

    python3 ci/record-gateway.py <out-dir> [port]

`antstream-visual` has no funded postage batch, so the app's in-process
gateway refuses every upload and the publisher never gets as far as a
playlist. Launched with `-antstreamShotPublisherGateway
http://127.0.0.1:<port>`, the app's publisher posts here instead:

* every `POST` (segments, playlists, feed updates) is answered `201` with
  a made-up 64-hex reference, so segments "land" and get listed;
* a body that is a playlist (`#EXTM3U`) is also written to
  `<out-dir>/playlist-<n>.m3u8`, in the order it arrived;
* `GET /feeds/...` answers `404 {"message":"no update found"}` (a feed
  nothing was written to: the broadcast starts at index 0), any other
  `GET` a plain 404.

Nothing is stored, so a playlist's own upload fails its root-chunk check
in the publisher and no feed update goes out: the point is only to read
what the publisher listed (which `#EXT-X-MAP`s, where the
`#EXT-X-DISCONTINUITY` is), which it renders before uploading.
"""

import hashlib
import itertools
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

OUT = sys.argv[1]
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 18633
COUNTER = itertools.count(1)
os.makedirs(OUT, exist_ok=True)


class Handler(BaseHTTPRequestHandler):
    # HTTP/1.0: the connection closes after each answer, which is what the
    # publisher's minimal client reads to.
    protocol_version = "HTTP/1.0"

    def answer(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length)
        n = next(COUNTER)
        if body.startswith(b"#EXTM3U"):
            with open(os.path.join(OUT, f"playlist-{n:05d}.m3u8"), "wb") as f:
                f.write(body)
        reference = hashlib.sha256(n.to_bytes(8, "big") + body).hexdigest()
        self.answer(201, {"reference": reference})

    def do_GET(self):
        if self.path.startswith("/feeds/"):
            self.answer(404, {"message": "no update found", "code": 404})
        else:
            self.answer(404, {"message": "Not Found", "code": 404})

    def log_message(self, fmt, *args):
        sys.stderr.write("record-gateway: " + (fmt % args) + "\n")


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
