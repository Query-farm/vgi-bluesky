"""Serve a deliberately projected public response; never contact an upstream API."""

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from urllib.parse import parse_qs, urlparse


def start_replay() -> ThreadingHTTPServer:
    """Start an ephemeral loopback replay for the one supported author-feed request."""
    recording = json.loads((Path(__file__).parent / "data/official-feed.json").read_text())
    payload = json.dumps(recording["response"]).encode()

    class Handler(BaseHTTPRequestHandler):
        """Respond only to the recorded request; reject other requests explicitly."""

        def log_message(self, *_args: object) -> None:
            """Keep HTTP logs out of notebook output."""

        def do_GET(self) -> None:
            """Return the projected recording or an explicit unsupported-request error."""
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            supported = (
                parsed.path == "/xrpc/app.bsky.feed.getAuthorFeed"
                and query.get("actor") == ["bsky.app"]
                and query.get("filter") == ["posts_no_replies"]
                and query.get("limit") == ["100"]
                and query.get("includePins", ["false"]) == ["false"]
                and set(query) <= {"actor", "filter", "limit", "includePins"}
            )
            body = payload if supported else b'{"error":"Replay supports only the documented bounded feed request"}'
            self.send_response(200 if supported else 400)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    Thread(target=server.serve_forever, daemon=True).start()
    return server
