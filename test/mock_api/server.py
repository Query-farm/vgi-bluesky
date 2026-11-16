#!/usr/bin/env python3
"""A local, synthetic Bluesky AppView for test/sql/*.test.

Serves the public-read XRPC queries the worker calls (the same ``/xrpc/<nsid>``
shapes the AppView answers) over a small authored world: four accounts, a
three-post thread, a quote post, a repost, two feed generators and two trends.
Nothing here is recorded from, or forwarded to, the real network.

ci/run-integration.sh points both ``BLUESKY_APPVIEW_URL`` and
``BLUESKY_SEARCH_URL`` at it. Every cursor-paged list returns at most
:data:`PAGE_CAP` rows per page, so the SQL suite exercises the worker's cursor
walk (except ``searchPosts``, which the worker reads one page of by design).
Unknown actors, posts and feeds answer with the AppView's own 400 error bodies.
Stdlib only, so it runs under ``uv run --no-project``.

Usage::

    python3 test/mock_api/server.py            # prints PORT:<n> once listening
"""

from __future__ import annotations

import argparse
import contextlib
import json
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

PAGE_CAP = 2


def _did(c: str) -> str:
    return "did:plc:" + c * 24


ACTORS: dict[str, dict[str, Any]] = {
    "alice.test": {
        "did": _did("a"),
        "handle": "alice.test",
        "displayName": "Alice Analyst",
        "description": "I write about databases and DuckDB.",
        "createdAt": "2024-01-02T03:04:05.000Z",
        "indexedAt": "2026-09-01T00:00:00.000Z",
        "labels": [],
        "followersCount": 3,
        "followsCount": 1,
        "postsCount": 2,
        "website": "https://alice.example",
        "associated": {"lists": 1, "feedgens": 2, "starterPacks": 0, "labeler": False},
        "verification": {"verifiedStatus": "valid"},
    },
    "bob.test": {
        "did": _did("b"),
        "handle": "bob.test",
        "displayName": "Bob Builder",
        "description": "Data engineer.",
        "createdAt": "2024-02-03T00:00:00.000Z",
        "indexedAt": "2026-09-01T00:00:00.000Z",
        "labels": [],
        "followersCount": 1,
        "followsCount": 2,
        "postsCount": 1,
        "associated": {"lists": 0, "feedgens": 0, "starterPacks": 0, "labeler": False},
        "verification": {"verifiedStatus": "none"},
    },
    "carol.test": {
        "did": _did("c"),
        "handle": "carol.test",
        "displayName": "Carol Coder",
        "description": "Quotes everything.",
        "createdAt": "2024-03-04T00:00:00.000Z",
        "indexedAt": "2026-09-01T00:00:00.000Z",
        "labels": [{"val": "!no-unauthenticated"}],
        "followersCount": 1,
        "followsCount": 1,
        "postsCount": 2,
        "associated": {"lists": 0, "feedgens": 0, "starterPacks": 1, "labeler": False},
    },
    "dave.test": {
        "did": _did("d"),
        "handle": "dave.test",
        "displayName": "Dave Duck",
        "description": "Ducks all the way down.",
        "createdAt": "2024-04-05T00:00:00.000Z",
        "indexedAt": "2026-09-01T00:00:00.000Z",
        "labels": [],
        "followersCount": 0,
        "followsCount": 1,
        "postsCount": 0,
        "associated": {"lists": 0, "feedgens": 0, "starterPacks": 0, "labeler": True},
    },
}
BY_DID = {a["did"]: a for a in ACTORS.values()}
_BASIC_KEYS = ("did", "handle", "displayName", "description", "createdAt", "indexedAt", "labels")


def basic(handle: str) -> dict[str, Any]:
    """The basic actor view (search results, followers, likers)."""
    return {k: ACTORS[handle][k] for k in _BASIC_KEYS if k in ACTORS[handle]}


#: (followed, follower): bob, carol and dave follow alice; alice follows bob.
FOLLOWS = [
    ("alice.test", "bob.test"),
    ("alice.test", "carol.test"),
    ("alice.test", "dave.test"),
    ("bob.test", "alice.test"),
]


def _uri(handle: str, rkey: str) -> str:
    return f"at://{ACTORS[handle]['did']}/app.bsky.feed.post/{rkey}"


ROOT_URI = _uri("alice.test", "3root")
REPLY_URI = _uri("bob.test", "3reply")
NESTED_URI = _uri("carol.test", "3nested")
QUOTE_URI = _uri("carol.test", "3quote")
SOLO_URI = _uri("alice.test", "3solo")


def _post(
    handle: str, rkey: str, text: str, created: str, counts: tuple[int, int, int, int], **record: Any
) -> dict[str, Any]:
    author = ACTORS[handle]
    replies, reposts, likes, quotes = counts
    return {
        "uri": _uri(handle, rkey),
        "cid": f"bafy{rkey}",
        "author": {"did": author["did"], "handle": handle, "displayName": author["displayName"]},
        "record": {
            "$type": "app.bsky.feed.post",
            "text": text,
            "createdAt": created,
            "langs": ["en"],
            **record,
        },
        "replyCount": replies,
        "repostCount": reposts,
        "likeCount": likes,
        "quoteCount": quotes,
        "bookmarkCount": 0,
        "indexedAt": created,
        "labels": [],
    }


POSTS = {
    ROOT_URI: _post(
        "alice.test",
        "3root",
        "DuckDB 2.0 is out #duckdb",
        "2026-09-01T10:00:00.000Z",
        (1, 1, 2, 1),
        facets=[
            {
                "features": [
                    {"$type": "app.bsky.richtext.facet#tag", "tag": "duckdb"},
                    {"$type": "app.bsky.richtext.facet#link", "uri": "https://duckdb.org"},
                ]
            }
        ],
    ),
    REPLY_URI: _post(
        "bob.test",
        "3reply",
        "Congrats @alice.test!",
        "2026-09-01T11:00:00.000Z",
        (1, 0, 1, 0),
        reply={"parent": {"uri": ROOT_URI}, "root": {"uri": ROOT_URI}},
        facets=[{"features": [{"$type": "app.bsky.richtext.facet#mention", "did": _did("a")}]}],
    ),
    NESTED_URI: _post(
        "carol.test",
        "3nested",
        "Agreed",
        "2026-09-01T12:00:00.000Z",
        (0, 0, 0, 0),
        reply={"parent": {"uri": REPLY_URI}, "root": {"uri": ROOT_URI}},
    ),
    QUOTE_URI: _post(
        "carol.test",
        "3quote",
        "Worth reading",
        "2026-09-02T09:00:00.000Z",
        (0, 0, 0, 0),
        embed={"$type": "app.bsky.embed.record", "record": {"uri": ROOT_URI, "cid": "bafy3root"}},
    ),
    SOLO_URI: _post(
        "alice.test",
        "3solo",
        "Parquet tips for the weekend",
        "2026-08-30T08:00:00.000Z",
        (0, 0, 0, 0),
        langs=["en", "de"],
        embed={"$type": "app.bsky.embed.images", "images": [{"alt": "a"}, {"alt": "b"}]},
    ),
}
LIKES = {
    ROOT_URI: [("bob.test", "2026-09-01T13:00:00.000Z"), ("carol.test", "2026-09-01T14:00:00.000Z")],
    REPLY_URI: [("alice.test", "2026-09-01T15:00:00.000Z")],
}
REPOSTS = {ROOT_URI: ["dave.test"]}
QUOTES = {ROOT_URI: [QUOTE_URI]}

#: getAuthorFeed: each account's own posts, newest first; dave's is a repost of the root.
AUTHOR_FEEDS: dict[str, list[dict[str, Any]]] = {
    "alice.test": [{"uri": ROOT_URI}, {"uri": SOLO_URI}],
    "bob.test": [{"uri": REPLY_URI}],
    "carol.test": [{"uri": QUOTE_URI}, {"uri": NESTED_URI}],
    "dave.test": [{"uri": ROOT_URI, "repost": "2026-09-01T16:00:00.000Z"}],
}

FEED_GENS = [
    {
        "uri": f"at://{_did('a')}/app.bsky.feed.generator/duck-news",
        "cid": "bafyfeed1",
        "did": "did:web:feeds.alice.example",
        "creator": basic("alice.test"),
        "displayName": "Duck News",
        "description": "Posts about DuckDB",
        "likeCount": 42,
        "acceptsInteractions": True,
        "indexedAt": "2026-08-01T00:00:00.000Z",
        "labels": [],
    },
    {
        "uri": f"at://{_did('a')}/app.bsky.feed.generator/parquet",
        "cid": "bafyfeed2",
        "did": "did:web:feeds.alice.example",
        "creator": basic("alice.test"),
        "displayName": "Parquet Pics",
        "description": "Columnar videos",
        "likeCount": 7,
        "contentMode": "app.bsky.feed.defs#contentModeVideo",
        "indexedAt": "2026-08-02T00:00:00.000Z",
        "labels": [],
    },
]
FEED_CONTENTS = {FEED_GENS[0]["uri"]: [ROOT_URI, QUOTE_URI], FEED_GENS[1]["uri"]: [SOLO_URI]}

TRENDS = [
    {
        "topic": "duckdb-2",
        "displayName": "DuckDB 2.0",
        "description": "A new major release",
        "link": "/profile/trending.bsky.app/feed/duckdb-2",
        "startedAt": "2026-09-01T09:00:00.000Z",
        "postCount": 1200,
        "status": "hot",
        "category": "tech",
        "actors": [basic("alice.test"), basic("bob.test")],
    },
    {
        "topic": "parquet",
        "displayName": "Parquet",
        "link": "/profile/trending.bsky.app/feed/parquet",
        "startedAt": "2026-09-01T10:00:00.000Z",
        "postCount": 300,
        "status": "cooling",
        "category": "tech",
        "actors": [],
    },
]


class NotFound(Exception):
    """An XRPC 400 with an error name, as the AppView answers unknown things."""

    def __init__(self, error: str, message: str) -> None:
        super().__init__(message)
        self.error = error
        self.message = message


def actor_of(value: str, *, missing: str = "Actor not found") -> str:
    """The handle for a handle or DID, or the AppView's not-found error."""
    if value in ACTORS:
        return value
    if value in BY_DID:
        return BY_DID[value]["handle"]
    if not re.fullmatch(r"did:[a-z]+:[A-Za-z0-9._:%-]+|[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+", value):
        raise NotFound(
            "InvalidRequest", f"Error: actor must be a valid did or a handle: Invalid AT identifier: {value}"
        )
    raise NotFound("InvalidRequest", f"{missing}: {value}" if missing.startswith("Actor") else missing)


def _page(rows: list[Any], q: dict[str, str], key: str, cap: int = PAGE_CAP, **extra: Any) -> dict[str, Any]:
    start = int(q.get("cursor") or 0)
    size = min(int(q.get("limit", cap)), cap)
    chunk = rows[start : start + size]
    out = {key: chunk, **extra}
    if start + size < len(rows):
        out["cursor"] = str(start + size)
    return out


def _thread_node(uri: str, depth: int) -> dict[str, Any]:
    node: dict[str, Any] = {"$type": "app.bsky.feed.defs#threadViewPost", "post": POSTS[uri]}
    if depth > 0:
        children = [
            u for u, p in POSTS.items() if p["record"].get("reply", {}).get("parent", {}).get("uri") == uri
        ]
        node["replies"] = [_thread_node(u, depth - 1) for u in children]
    return node


def route(method: str, query: dict[str, list[str]]) -> dict[str, Any]:  # noqa: C901 - one switch
    q = {k: v[-1] for k, v in query.items()}
    if method == "com.atproto.identity.resolveHandle":
        handle = q.get("handle", "")
        if handle not in ACTORS:
            raise NotFound("InvalidRequest", "Unable to resolve handle")
        return {"did": ACTORS[handle]["did"]}
    if method == "app.bsky.actor.getProfiles":
        found = []
        for a in query.get("actors", []):
            with contextlib.suppress(NotFound):
                found.append(dict(ACTORS[actor_of(a)]))
        return {"profiles": found}
    if method == "app.bsky.actor.searchActors":
        term = q.get("q", "").lower()
        rows = [
            basic(h)
            for h, a in ACTORS.items()
            if term in h or term in a["displayName"].lower() or term in a["description"].lower()
        ]
        return _page(rows, q, "actors")
    if method in ("app.bsky.graph.getFollowers", "app.bsky.graph.getFollows"):
        who = actor_of(q.get("actor", ""))
        if method.endswith("Followers"):
            rows = [basic(f) for t, f in FOLLOWS if t == who]
            return _page(rows, q, "followers", subject=basic(who))
        rows = [basic(t) for t, f in FOLLOWS if f == who]
        return _page(rows, q, "follows", subject=basic(who))
    if method == "app.bsky.feed.getAuthorFeed":
        who = actor_of(q.get("actor", ""), missing="Profile not found")
        items = []
        for entry in AUTHOR_FEEDS[who]:
            post = POSTS[entry["uri"]]
            if q.get("filter") == "posts_no_replies" and "reply" in post["record"]:
                continue
            if q.get("filter") == "posts_with_media" and "images" not in post["record"].get("embed", {}).get(
                "$type", ""
            ):
                continue
            item: dict[str, Any] = {"post": post}
            if "repost" in entry:
                item["reason"] = {
                    "$type": "app.bsky.feed.defs#reasonRepost",
                    "by": basic(who),
                    "indexedAt": entry["repost"],
                }
            items.append(item)
        return _page(items, q, "feed")
    if method == "app.bsky.feed.getActorFeeds":
        who = actor_of(q.get("actor", ""), missing="Profile not found")
        return _page([g for g in FEED_GENS if g["creator"]["handle"] == who], q, "feeds")
    if method == "app.bsky.feed.getFeed":
        uri = q.get("feed", "")
        if uri not in FEED_CONTENTS:
            raise NotFound("UnknownFeed", f"Unknown feed: {uri}")
        return _page([{"post": POSTS[u]} for u in FEED_CONTENTS[uri]], q, "feed")
    if method == "app.bsky.unspecced.getPopularFeedGenerators":
        term = q.get("query", "").lower()
        rows = [g for g in FEED_GENS if term in g["displayName"].lower() or term in g["description"].lower()]
        return _page(rows, q, "feeds")
    if method == "app.bsky.unspecced.getTrends":
        return {"trends": TRENDS}
    if method == "app.bsky.feed.getPosts":
        return {"posts": [POSTS[u] for u in query.get("uris", []) if u in POSTS]}
    if method == "app.bsky.feed.getPostThread":
        uri = q.get("uri", "")
        if uri not in POSTS:
            raise NotFound("NotFound", f"Post not found: {uri}")
        node = _thread_node(uri, int(q.get("depth", 6)))
        cursor, height = node, int(q.get("parentHeight", 80))
        while height > 0 and (parent := POSTS[cursor["post"]["uri"]]["record"].get("reply")):
            cursor["parent"] = {
                "$type": "app.bsky.feed.defs#threadViewPost",
                "post": POSTS[parent["parent"]["uri"]],
            }
            cursor, height = cursor["parent"], height - 1
        return {"thread": node}
    if method == "app.bsky.feed.getLikes":
        uri = q.get("uri", "")
        rows = [{"actor": basic(h), "createdAt": t, "indexedAt": t} for h, t in LIKES.get(uri, [])]
        return _page(rows, q, "likes", uri=uri)
    if method == "app.bsky.feed.getRepostedBy":
        uri = q.get("uri", "")
        return _page([basic(h) for h in REPOSTS.get(uri, [])], q, "repostedBy", uri=uri)
    if method == "app.bsky.feed.getQuotes":
        uri = q.get("uri", "")
        return _page([POSTS[u] for u in QUOTES.get(uri, [])], q, "posts", uri=uri)
    if method == "app.bsky.feed.searchPosts":
        term = q.get("q", "").lower()
        rows = []
        for post in sorted(POSTS.values(), key=lambda p: p["record"]["createdAt"], reverse=True):
            rec = post["record"]
            if term and term not in rec["text"].lower():
                continue
            if (author := q.get("author")) and author not in (
                post["author"]["handle"],
                post["author"]["did"],
            ):
                continue
            if (lang := q.get("lang")) and lang not in rec["langs"]:
                continue
            if (since := q.get("since")) and rec["createdAt"] < since:
                continue
            if (until := q.get("until")) and rec["createdAt"] >= until:
                continue
            rows.append(post)
        if q.get("sort") == "top":
            rows.sort(key=lambda p: p["likeCount"], reverse=True)
        return _page(rows, q, "posts", cap=100)
    raise NotFound("MethodNotImplemented", f"Method not implemented: {method}")


class Handler(BaseHTTPRequestHandler):
    """Serve ``/xrpc/<nsid>`` GETs; refuse every other verb."""

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - stdlib signature
        """Log to stderr only, so stdout carries nothing except the ``PORT:`` line."""
        sys.stderr.write("mock_api: " + (format % args) + "\n")

    def _send(self, status: int, body: object) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:
        """Answer one XRPC query."""
        url = urlparse(self.path)
        if not url.path.startswith("/xrpc/"):
            self._send(404, {"error": "NotFound", "message": "not an XRPC path"})
            return
        try:
            self._send(200, route(url.path.removeprefix("/xrpc/"), parse_qs(url.query)))
        except NotFound as exc:
            self._send(400, {"error": exc.error, "message": exc.message})

    def do_POST(self) -> None:
        """The worker is read-only; a write reaching the mock is a bug."""
        self._send(405, {"error": "MethodNotAllowed", "message": "the worker must never write"})


def main() -> None:
    """Start the server and print ``PORT:<n>`` once it is listening."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    with contextlib.suppress(KeyboardInterrupt):
        server.serve_forever()


if __name__ == "__main__":
    main()
