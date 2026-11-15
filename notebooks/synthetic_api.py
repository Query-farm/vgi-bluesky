"""Serve a bounded authored Bluesky model on loopback; never contact the network."""

import copy
import hashlib
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock, Thread
from urllib.parse import parse_qs, urlsplit


class SyntheticAPI:
    """Public-read XRPC shapes backed by one explicit synthetic JSON companion."""

    def __init__(self, path: Path):
        self.model = json.loads(path.read_text())
        self.requests = []
        self._lock = Lock()
        self._closed = False
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_GET(self):
                parsed = urlsplit(self.path)
                query = parse_qs(parsed.query)
                try:
                    payload = owner.response(parsed.path, query)
                    status = 200
                except (ValueError, KeyError, TypeError):
                    payload = {"error": "Unsupported synthetic request; no upstream fallback"}
                    status = 400
                body = json.dumps(payload, separators=(",", ":")).encode()
                with owner._lock:
                    owner.requests.append(
                        {
                            "path": parsed.path,
                            "parameters": query,
                            "status": status,
                            "bytes": len(body),
                            "sha256": hashlib.sha256(body).hexdigest(),
                        }
                    )
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def response(self, path, query):
        """Validate query contracts before returning a locally authored response."""
        prefix = "/xrpc/"
        if not path.startswith(prefix):
            raise ValueError("Unknown endpoint")
        method = path[len(prefix) :]
        if method == "app.bsky.actor.searchActors":
            if query != {"q": ["publisher"], "limit": ["100"]}:
                raise ValueError("Unknown directory query")
            return {"actors": self.model["directory"]}
        if method == "app.bsky.feed.searchPosts":
            if query != {
                "q": ["survey"],
                "sort": ["latest"],
                "author": ["did:plc:" + "p" * 24],
                "limit": ["100"],
            }:
                raise ValueError("Unknown post query")
            return {"posts": self.model["search_posts"]}
        if method == "app.bsky.actor.getProfiles":
            if set(query) != {"actors"}:
                raise ValueError("Unknown profile query")
            found = {p["did"]: p for p in self.model["profiles"]}
            if not set(query["actors"]) <= set(found):
                raise ValueError("Unknown synthetic actor")
            return {"profiles": [found[x] for x in query["actors"]]}
        if method == "app.bsky.feed.getPosts":
            if set(query) != {"uris"}:
                raise ValueError("Unknown post lookup")
            found = {p["uri"]: p for p in self.model["posts"]}
            if not set(query["uris"]) <= set(found):
                raise ValueError("Unknown synthetic post")
            return {"posts": [found[x] for x in query["uris"]]}
        if query.get("uri") != [self.model["anchor_uri"]]:
            raise ValueError("Unknown anchor")
        if method == "app.bsky.feed.getPostThread":
            if set(query) != {"uri", "depth", "parentHeight"} or query["parentHeight"] != ["0"]:
                raise ValueError("Unknown thread query")
            depth = int(query["depth"][0])
            if depth not in (1, 2):
                raise ValueError("Unsupported depth")
            tree = copy.deepcopy(self.model["thread"])

            def prune(node, remaining):
                if remaining == 0:
                    node.pop("replies", None)
                else:
                    for child in node.get("replies", []):
                        prune(child, remaining - 1)

            prune(tree, depth)
            return {"thread": tree}
        if set(query) != {"uri", "limit"} or query["limit"] != ["100"]:
            raise ValueError("Unknown listing query")
        if method == "app.bsky.feed.getLikes":
            return {"uri": self.model["anchor_uri"], "likes": self.model["likes"]}
        if method == "app.bsky.feed.getRepostedBy":
            return {"uri": self.model["anchor_uri"], "repostedBy": self.model["repostedBy"]}
        if method == "app.bsky.feed.getQuotes":
            return {"posts": self.model["quotes"]}
        raise ValueError("Unknown endpoint")

    def close(self):
        """Stop and close the local fixture; safe to call again during cleanup."""
        if not self._closed:
            self._closed = True
            self.server.shutdown()
            self.server.server_close()
            self.thread.join(timeout=2)
