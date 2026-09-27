"""Read-only HTTP access to Bluesky's public AppView (XRPC).

Every function here issues ``GET`` requests only — the single chokepoint
:func:`_get` is the sole place an HTTP call is made, which is what the read-only
CI guard (``tests/test_readonly_guard.py``) asserts against. XRPC splits its
methods into *queries* (``GET``) and *procedures* (``POST``); every write in the
AT Protocol — creating a post, a like, a follow, even logging in — is a
procedure, so a client that can only issue ``GET`` cannot write by construction.

No credentials are used. The whole surface this worker exposes is served
unauthenticated by ``public.api.bsky.app``, a CDN-fronted read replica of the
AppView, with one exception: ``app.bsky.feed.searchPosts`` answers 403 there and
is served from ``api.bsky.app`` instead, which accepts it anonymously.

Unlike a REST API, XRPC puts every argument in the query string rather than the
path — the path is always ``/xrpc/<nsid>`` with a fixed method name — so a value
from user SQL cannot traverse out of its position the way a path segment can.
"""

from __future__ import annotations

import atexit
import json
import os
import re
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote, urlparse

import httpx

#: The CDN-fronted, unauthenticated read replica of the Bluesky AppView.
DEFAULT_APPVIEW_URL = "https://public.api.bsky.app"

#: ``app.bsky.feed.searchPosts`` is refused (HTTP 403) on the public replica but
#: served anonymously by the main AppView host, so search alone goes here.
DEFAULT_SEARCH_URL = "https://api.bsky.app"

#: Every paged Bluesky list endpoint caps ``limit`` at 100 and answers HTTP 400
#: ``integer too big`` above it rather than clamping.
PAGE_LIMIT = 100

#: ``getProfiles`` and ``getPosts`` take at most 25 actors / URIs per call and
#: answer 400 ``array too big`` above it.
BATCH_LIMIT = 25

#: ``getTrends`` caps at 25 rows, and has no cursor: this is the whole list.
TRENDS_LIMIT = 25

#: Connect and read timeouts for every request this module makes.
TIMEOUT = httpx.Timeout(30.0, connect=10.0)

#: Grace window (seconds) for serving a stale cached result when a refetch
#: fails. Shared by every function that advertises cacheability.
STALE_IF_ERROR = 300

#: ``max-age=N``, anchored to a directive boundary so it cannot match the tail
#: of some other token.
_MAX_AGE = re.compile(r"(?:^|[\s,])max-age=(\d+)")

#: Directives that forbid reuse outright, whatever ``max-age`` also says.
_NO_REUSE = re.compile(r"(?:^|[\s,])(?:no-store|no-cache|private)(?:\s*[,=]|\s*$)")

#: Statuses worth retrying: the rate limiter, plus the CDN's transient 5xx.
_RATE_LIMITED = 429
_RETRYABLE_STATUSES = frozenset({_RATE_LIMITED, 502, 503, 504})

#: Exponential backoff for a retryable request: ~0.5s, 1s, 2s, 4s, 8s.
_RETRY_ATTEMPTS = 5
_RETRY_BASE_SECONDS = 0.5

#: Never sleep longer than this on a server-supplied ``Retry-After``; a longer
#: wait is better reported as an error than silently absorbed by a query.
_MAX_RETRY_AFTER_SECONDS = 30.0

#: The XRPC method names this module is allowed to call. Every one is a
#: ``query`` in its lexicon; the read-only guard asserts nothing else appears.
READ_METHODS = frozenset(
    {
        "app.bsky.actor.getProfiles",
        "app.bsky.actor.searchActors",
        "app.bsky.feed.getActorFeeds",
        "app.bsky.feed.getAuthorFeed",
        "app.bsky.feed.getFeed",
        "app.bsky.feed.getLikes",
        "app.bsky.feed.getPostThread",
        "app.bsky.feed.getPosts",
        "app.bsky.feed.getQuotes",
        "app.bsky.feed.getRepostedBy",
        "app.bsky.feed.searchPosts",
        "app.bsky.graph.getFollowers",
        "app.bsky.graph.getFollows",
        "app.bsky.unspecced.getPopularFeedGenerators",
        "app.bsky.unspecced.getTrends",
        "com.atproto.identity.resolveHandle",
    }
)


@dataclass(slots=True)
class CacheHint:
    """The origin's own freshness opinion, collected across a call's responses.

    ``public.api.bsky.app`` sets ``Cache-Control: public, max-age=30`` on every
    successful query; ``api.bsky.app`` (search) sends none. Passing one of these
    into an API call lets a table function forward the origin's actual policy to
    DuckDB's result cache instead of inventing one.

    ``max_age`` is the **minimum** seen across every response folded into it, so
    the shortest-lived response bounds the result. It stays ``None`` when the
    origin declared nothing, which is itself the signal that the data is live.
    """

    max_age: int | None = None
    #: True once any response arrived without a usable freshness directive.
    saw_uncacheable: bool = field(default=False)

    def observe(self, response: httpx.Response) -> None:
        """Fold one response's Cache-Control into the running hint.

        ``max-age=0`` is "already stale", not a cache entry worth holding —
        treating it as one would pair a zero lifetime with
        :data:`STALE_IF_ERROR`, licensing stale serving the origin never asked for.
        """
        directives = response.headers.get("cache-control", "")
        match = _MAX_AGE.search(directives)
        if match is None or _NO_REUSE.search(directives):
            self.saw_uncacheable = True
            return
        seconds = int(match.group(1))
        if seconds == 0:
            self.saw_uncacheable = True
            return
        self.max_age = seconds if self.max_age is None else min(self.max_age, seconds)

    @property
    def cacheable(self) -> bool:
        """Whether every response in this call carried a usable freshness directive."""
        return self.max_age is not None and not self.saw_uncacheable


def appview_url() -> str:
    """The AppView base URL, overridable with ``BLUESKY_APPVIEW_URL``."""
    return os.environ.get("BLUESKY_APPVIEW_URL", DEFAULT_APPVIEW_URL).rstrip("/")


def search_url() -> str:
    """The base URL for post search, overridable with ``BLUESKY_SEARCH_URL``."""
    return os.environ.get("BLUESKY_SEARCH_URL", DEFAULT_SEARCH_URL).rstrip("/")


def open_client() -> httpx.Client:
    """Open a client carrying this module's timeouts.

    Callers that issue a burst of per-row fetches (a correlated LATERAL) should
    open one of these and pass it in, so the whole batch shares a connection
    pool. It is the only httpx object the rest of the package constructs.
    """
    return httpx.Client(timeout=TIMEOUT)


_shared: httpx.Client | None = None
_shared_lock = threading.Lock()


def shared_client() -> httpx.Client:
    """A process-wide client, for calls that have no batch to share one with.

    A paged scan fetches one page per ``process()`` tick, so there is no
    enclosing scope to hold a client open across the walk; without this every
    page would pay a fresh TLS handshake.
    """
    global _shared
    if _shared is None or _shared.is_closed:
        with _shared_lock:
            if _shared is None or _shared.is_closed:
                _shared = open_client()
    return _shared


def reset_shared_client() -> None:
    """Close and forget the process pool, so the next call opens a fresh one.

    Operationally it forces a reconnect; in tests it keeps the pool hermetic, so
    a client built against one test's mock transport cannot serve the next.
    """
    global _shared
    with _shared_lock:
        if _shared is not None and not _shared.is_closed:
            _shared.close()
        _shared = None


atexit.register(reset_shared_client)


class BlueskyError(RuntimeError):
    """A non-2xx XRPC response, carrying the status, method and Bluesky's own error.

    XRPC errors are JSON — ``{"error": "InvalidRequest", "message": "Profile not
    found"}`` — so the message is built from those two fields rather than from
    the raw body, which is what a DuckDB user reads first.
    """

    def __init__(self, status: int, method: str, body: str) -> None:
        self.status = status
        self.method = method
        self.error, self.message = _xrpc_error(body)
        short = method.rsplit(".", 1)[-1]
        detail = f"{self.error}: {self.message}" if self.error else self.message
        super().__init__(f"Bluesky {short} failed (HTTP {status}): {detail[:400]}")


def _xrpc_error(body: str) -> tuple[str, str]:
    """``(error, message)`` from an XRPC error body, falling back to the raw text."""
    try:
        payload = json.loads(body)
    except ValueError:
        return "", body.strip()
    if not isinstance(payload, dict):
        return "", body.strip()
    return str(payload.get("error") or ""), str(payload.get("message") or body.strip())


class ActorNotFoundError(ValueError):
    """An actor argument named no Bluesky account."""


class InvalidActorError(ValueError):
    """An actor argument was not a handle, DID or profile URL at all."""


#: How XRPC words "no such account": "Profile not found" (getAuthorFeed,
#: getActorFeeds) and "Actor not found: <actor>" (getFollowers, getFollows).
_NOT_FOUND = re.compile(r"^(Profile|Actor) not found")


def actor_error(exc: BlueskyError, actor: str, *, client: httpx.Client | None = None) -> ValueError | None:
    """A clear error for a failed call about ``actor``, or None if it was not the actor's fault.

    An unknown handle is almost always a near-miss — ``medriscoll.bsky.social``
    for ``medriscoll.com`` — so the account search is asked once, on this error
    path only, for accounts matching the handle's first label, and the closest
    few are suggested by handle and display name.
    """
    if exc.status != 400:
        return None
    if "Invalid AT identifier" in exc.message:
        return InvalidActorError(
            f"{actor!r} is not a Bluesky account identifier. Pass a handle such as 'bsky.app', "
            "a DID such as 'did:plc:…', '@handle', or a https://bsky.app/profile/… URL."
        )
    if not _NOT_FOUND.match(exc.message):
        return None
    message = f"No Bluesky account {actor!r}."
    if not actor.startswith("did:") and (term := actor.split(".")[0].strip()):
        try:
            found = _get("app.bsky.actor.searchActors", {"q": term, "limit": 3}, client=client)
        except (BlueskyError, httpx.HTTPError):
            found = {}
        guesses = [
            f"{a['handle']!r} ({a['displayName']})" if a.get("displayName") else repr(a["handle"])
            for a in found.get("actors") or []
            if isinstance(a, dict) and a.get("handle") and a.get("handle") != actor
        ]
        if guesses:
            message += f" Did you mean {' or '.join(guesses)}?"
    return ActorNotFoundError(f"{message} search_actors('…') finds accounts by name.")


def _retry_after(response: httpx.Response) -> float | None:
    """Seconds the server asked us to wait, when it said so in a usable form."""
    raw = response.headers.get("retry-after")
    if raw is None:
        return None
    try:
        return min(max(float(raw), 0.0), _MAX_RETRY_AFTER_SECONDS)
    except ValueError:
        return None


def _get(
    method: str,
    params: dict[str, Any] | Sequence[tuple[str, Any]] | None = None,
    *,
    base: str | None = None,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> dict[str, Any]:
    """GET one XRPC query and return the decoded JSON object.

    The one and only outbound-HTTP chokepoint. It issues ``GET`` and nothing
    else, and refuses any method not in :data:`READ_METHODS` — so even a
    procedure name assembled at runtime cannot be reached through it.

    A ``GET`` is idempotent, so every transient failure is retried with
    exponential backoff (or the server's ``Retry-After``, when it sends one):
    the rate limiter (429), the CDN's 5xx, and transport errors.

    Args:
        method: The XRPC method NSID, e.g. ``app.bsky.actor.getProfiles``.
        params: Query parameters; ``None`` values are dropped. A sequence of
            pairs rather than a mapping when a parameter must repeat
            (``?actors=a&actors=b``).
        base: Host to send the request to; defaults to :func:`appview_url`.
        client: Optional caller-owned client, so a lateral join can reuse one
            connection pool across a batch of per-row fetches.
        hint: Optional :class:`CacheHint` to fold this response's
            ``Cache-Control`` into.

    Raises:
        BlueskyError: The response status was not 2xx after retries, or a 2xx
            body was not JSON.
        ValueError: ``method`` is not a known read-only query.
        httpx.TransportError: Every attempt failed to reach the API.
    """
    if method not in READ_METHODS:
        raise ValueError(f"{method!r} is not a read-only XRPC query this worker may call")
    pairs = list(params.items()) if isinstance(params, dict) else list(params or ())
    clean: Any = [(k, v) for k, v in pairs if v is not None]
    url = f"{base or appview_url()}/xrpc/{method}"
    http = client or shared_client()
    for attempt in range(_RETRY_ATTEMPTS):
        last_attempt = attempt == _RETRY_ATTEMPTS - 1
        delay = _RETRY_BASE_SECONDS * (2**attempt)
        try:
            response = http.get(url, params=clean)
        except httpx.TransportError:
            if last_attempt:
                raise
        else:
            if response.status_code not in _RETRYABLE_STATUSES or last_attempt:
                break
            delay = _retry_after(response) or delay
        time.sleep(delay)
    # Only fold a served response into the freshness hint; a 400 carries the
    # CDN's error policy (max-age=5), not the resource's.
    if hint is not None and response.status_code < 400:
        hint.observe(response)
    if response.status_code >= 400:
        raise BlueskyError(response.status_code, method, response.text)
    try:
        return response.json()
    except ValueError as exc:
        raise BlueskyError(
            response.status_code,
            method,
            f"expected JSON, got {response.headers.get('content-type', 'no content-type')}: {response.text}",
        ) from exc


def page(
    method: str,
    key: str,
    params: dict[str, Any] | None = None,
    *,
    cursor: str | None = None,
    base: str | None = None,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    page_limit: int = PAGE_LIMIT,
) -> tuple[dict[str, Any], list[dict[str, Any]], str | None]:
    """Fetch exactly one page, returning the payload, its rows, and the next cursor.

    A table function holds the cursor in its scan state and calls this once per
    tick, so DuckDB sees rows from the first page rather than waiting for the
    last, and a ``LIMIT`` stops the walk early. The whole payload is returned
    too, because some endpoints carry context beside the rows — ``getFollowers``
    names its ``subject``, which the rows themselves do not repeat.

    A ``None`` cursor means this was the final page. An empty page ends the walk
    even when a cursor comes back with it: ``getLists`` has been seen returning
    an empty page *and* a cursor, which would otherwise loop.
    """
    page_params = {**(params or {}), "limit": page_limit}
    if cursor:
        page_params["cursor"] = cursor
    payload = _get(method, page_params, base=base, client=client, hint=hint)
    rows = [row for row in payload.get(key) or [] if isinstance(row, dict)]
    next_cursor = payload.get("cursor") or None
    return payload, rows, (next_cursor if rows else None)


# --------------------------------------------------------------------------
# Identifiers
# --------------------------------------------------------------------------


def normalize_actor(value: str) -> str:
    """An actor as the API wants it: a DID or a bare handle.

    Accepts the forms people paste — ``@alice.bsky.social``, a profile URL
    ``https://bsky.app/profile/alice.bsky.social`` — and strips them to the
    identifier. Handles are case-insensitive; DIDs are not, so only a handle is
    lowercased.
    """
    text = value.strip()
    if text.startswith(("https://", "http://")):
        parts = [p for p in urlparse(text).path.split("/") if p]
        if len(parts) >= 2 and parts[0] == "profile":
            text = unquote(parts[1])
    text = text.removeprefix("@")
    return text if text.startswith("did:") else text.lower()


#: ``at://<authority>/<collection>/<rkey>``.
_AT_URI = re.compile(r"^at://([^/]+)/([^/]+)/([^/?#]+)$")


def post_uri_parts(value: str) -> tuple[str, str, str] | None:
    """``(authority, collection, rkey)`` from an AT-URI or a bsky.app web URL.

    ``https://bsky.app/profile/<actor>/post/<rkey>`` is what people copy out of
    the app; ``at://<actor>/app.bsky.feed.post/<rkey>`` is what the API takes.
    Both are accepted. Returns ``None`` for anything that is neither.
    """
    text = value.strip()
    if text.startswith(("https://", "http://")):
        parts = [unquote(p) for p in urlparse(text).path.split("/") if p]
        if len(parts) == 4 and parts[0] == "profile" and parts[2] == "post":
            return normalize_actor(parts[1]), "app.bsky.feed.post", parts[3]
        if len(parts) == 4 and parts[0] == "profile" and parts[2] == "feed":
            return normalize_actor(parts[1]), "app.bsky.feed.generator", parts[3]
        return None
    match = _AT_URI.match(text)
    if match is None:
        return None
    return normalize_actor(match.group(1)), match.group(2), match.group(3)


def resolve_handle(handle: str, *, client: httpx.Client | None = None) -> str | None:
    """The DID a handle currently points at, or ``None`` if it resolves to nothing."""
    try:
        return _get("com.atproto.identity.resolveHandle", {"handle": handle}, client=client).get("did")
    except BlueskyError as exc:
        if exc.status == 400:
            return None
        raise


def canonical_uri(
    value: str, *, client: httpx.Client | None = None, dids: dict[str, str | None]
) -> str | None:
    """A post or feed reference as a DID-authority AT-URI, which every endpoint accepts.

    ``getPosts`` answers **HTTP 500** to an AT-URI whose authority is a handle
    rather than a DID, even though ``getPostThread`` accepts the same URI —
    so handles are resolved here once, through ``dids``, a per-call memo that a
    batch of URIs by the same author shares.

    Returns ``None`` when the value is not a post/feed reference or its handle
    does not resolve; the caller emits no row for it.
    """
    parts = post_uri_parts(value)
    if parts is None:
        return None
    authority, collection, rkey = parts
    if not authority.startswith("did:"):
        if authority not in dids:
            dids[authority] = resolve_handle(authority, client=client)
        did = dids[authority]
        if did is None:
            return None
        authority = did
    return f"at://{authority}/{collection}/{rkey}"


# --------------------------------------------------------------------------
# Batched lookups (blended functions)
# --------------------------------------------------------------------------


def profiles(
    actors: Sequence[str],
    *,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> list[dict[str, Any]]:
    """Detailed profiles for many actors, chunked at :data:`BATCH_LIMIT`.

    Unknown actors are silently omitted by the endpoint rather than failing the
    call, so the caller matches rows back to inputs by DID or handle.
    """
    unique = list(dict.fromkeys(a for a in actors if a))
    found: list[dict[str, Any]] = []
    for start in range(0, len(unique), BATCH_LIMIT):
        chunk = unique[start : start + BATCH_LIMIT]
        payload = _get("app.bsky.actor.getProfiles", [("actors", a) for a in chunk], client=client, hint=hint)
        found.extend(p for p in payload.get("profiles") or [] if isinstance(p, dict))
    return found


def posts(
    uris: Sequence[str],
    *,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> list[dict[str, Any]]:
    """Hydrated posts for many DID-authority AT-URIs, chunked at :data:`BATCH_LIMIT`.

    A deleted or unknown post is omitted, not an error.
    """
    unique = list(dict.fromkeys(u for u in uris if u))
    found: list[dict[str, Any]] = []
    for start in range(0, len(unique), BATCH_LIMIT):
        chunk = unique[start : start + BATCH_LIMIT]
        payload = _get("app.bsky.feed.getPosts", [("uris", u) for u in chunk], client=client, hint=hint)
        found.extend(p for p in payload.get("posts") or [] if isinstance(p, dict))
    return found


def post_thread(
    uri: str,
    *,
    depth: int,
    parent_height: int,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
) -> dict[str, Any] | None:
    """The thread around one post, or ``None`` when the post is not found."""
    try:
        payload = _get(
            "app.bsky.feed.getPostThread",
            {"uri": uri, "depth": depth, "parentHeight": parent_height},
            client=client,
            hint=hint,
        )
    except BlueskyError as exc:
        # `NotFound` is a 400 with an error name, not a 404.
        if exc.status == 400 and "NotFound" in str(exc):
            return None
        raise
    thread = payload.get("thread")
    return thread if isinstance(thread, dict) else None


def trends(*, client: httpx.Client | None = None, hint: CacheHint | None = None) -> list[dict[str, Any]]:
    """The current trending topics — the whole list, in one unpaginated call."""
    payload = _get("app.bsky.unspecced.getTrends", {"limit": TRENDS_LIMIT}, client=client, hint=hint)
    return [t for t in payload.get("trends") or [] if isinstance(t, dict)]
