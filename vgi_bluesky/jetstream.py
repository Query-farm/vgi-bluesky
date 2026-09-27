"""The Bluesky firehose, via Jetstream, as a bounded streaming scan.

The AT Protocol firehose (``com.atproto.sync.subscribeRepos``) is every commit
to every repository on the network, as binary CBOR/CAR frames. **Jetstream** is
Bluesky's JSON re-encoding of it: one WebSocket, one JSON object per event,
filterable by collection and by DID, and replayable from a microsecond cursor.
It needs no credentials.

A firehose never ends. By default the scan is bounded by a wall-clock budget
(``seconds``), and optionally by ``max_events``, a pushed-down upper bound on
``event_time`` / ``time_us``, or a ``LIMIT``. With ``seconds => 0`` it is not:
it streams a batch per tick until the caller stops asking. Either way each
``process()`` tick reads one batch — ``batch_events`` events or ``batch_ms`` of time,
whichever closes first — and emits it, so DuckDB sees rows as they arrive and a
``LIMIT`` or cancellation lands within one batch.

Measured against ``jetstream2.us-east.bsky.network``:

* About 400 events a second across the network, roughly two thirds of them likes.
* ``time_us`` is strictly increasing and unique within a connection, which is
  what makes an upper time bound a safe stopping rule.
* ``identity`` and ``account`` events arrive even when ``wantedCollections`` is
  set, so a collection filter is a superset of what a ``WHERE collection = ...``
  keeps — safe to push down.
* Replay reaches back about a day and a half; an older cursor silently starts at
  the oldest buffered event.

Read-only, like the rest of the package: this module opens a WebSocket and only
ever receives on it. Jetstream's one client-to-server message (an options
update) is never sent — filters travel in the connection URL instead.
"""

from __future__ import annotations

import atexit
import contextlib
import json
import os
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar
from urllib.parse import urlencode

import pyarrow as pa
from vgi.arguments import Arg
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import BindParams, ProcessParams, TableFunctionGenerator, init_single_worker
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import ClientConnection, connect

from vgi_bluesky.meta import docs, examples
from vgi_bluesky.pushdown import build_filtered, equality
from vgi_bluesky.schemas import JETSTREAM_SCHEMA, flatten_jetstream_event

#: Bluesky's public Jetstream instances. Cursors are per-instance clocks, so a
#: scan stays on the instance it started on; the others are only tried when the
#: first connection of a scan fails.
JETSTREAM_HOSTS = (
    "wss://jetstream2.us-east.bsky.network",
    "wss://jetstream1.us-east.bsky.network",
    "wss://jetstream1.us-west.bsky.network",
    "wss://jetstream2.us-west.bsky.network",
)


#: A pooled connection is abandoned — its scan hit a LIMIT or was cancelled, so
#: DuckDB stopped calling it — once it has sat unused for two of its own batch
#: windows plus this grace. Scaled per scan rather than fixed: a fixed limit
#: shorter than a scan's ``batch_ms`` would close a socket mid-batch.
_IDLE_GRACE_SECONDS = 5.0

#: How often the background reaper looks for abandoned connections.
_REAP_INTERVAL_SECONDS = 1.0

_OPEN_TIMEOUT = 10.0

#: Connection attempts per tick before the scan fails: ~0.5s, 1s, 2s, 4s apart.
_CONNECT_ATTEMPTS = 5
_CONNECT_BACKOFF_SECONDS = 0.5

#: How long a close waits for Jetstream's close frame. The websockets default is
#: 10 seconds, and against a firehose that is still pushing it takes all ten —
#: measured at 10.00s on every one of five closes — which landed on the end of a
#: scan as ten seconds of dead time. A stream we are done with needs no graceful
#: goodbye, so the wait is short and happens off the query's thread.
_CLOSE_TIMEOUT = 0.5

#: Jetstream refuses a ``wantedCollections`` list longer than 100 (verified: 101
#: is an HTTP 400 at the handshake) and documents a 10,000 cap on ``wantedDids``.
#: Checking here turns an opaque handshake failure into a message naming the limit.
_MAX_COLLECTIONS = 100
_MAX_DIDS = 10_000


def hosts() -> tuple[str, ...]:
    """Instances to try, in order; ``BLUESKY_JETSTREAM_URL`` pins a single one."""
    pinned = os.environ.get("BLUESKY_JETSTREAM_URL")
    return (pinned.rstrip("/"),) if pinned else JETSTREAM_HOSTS


def subscribe_url(host: str, *, collections: list[str], dids: list[str], cursor: int) -> str:
    """The ``/subscribe`` URL for one connection. Every filter is a query parameter."""
    params: list[tuple[str, str]] = [("wantedCollections", c) for c in collections]
    params += [("wantedDids", d) for d in dids]
    if cursor > 0:
        params.append(("cursor", str(cursor)))
    query = urlencode(params)
    return f"{host}/subscribe" + (f"?{query}" if query else "")


# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _Pooled:
    socket: ClientConnection
    url: str
    last_used: float
    #: Seconds unused after which this connection counts as abandoned.
    idle_limit: float


_pool: dict[str, _Pooled] = {}
_pool_lock = threading.Lock()


def _close_quietly(socket: ClientConnection) -> None:
    """Close in the background so no scan ever waits on the closing handshake."""

    def close() -> None:
        with contextlib.suppress(Exception):
            socket.close()

    threading.Thread(target=close, name="jetstream-close", daemon=True).start()


def _reap(now: float) -> None:
    """Close connections an abandoned scan left behind. Caller holds the lock."""
    for key in [k for k, p in _pool.items() if now - p.last_used > p.idle_limit]:
        _close_quietly(_pool.pop(key).socket)


def _reaper() -> None:
    while True:
        time.sleep(_REAP_INTERVAL_SECONDS)
        with _pool_lock:
            _reap(time.monotonic())


threading.Thread(target=_reaper, name="jetstream-reaper", daemon=True).start()


def close_all() -> None:
    """Close every pooled connection (at exit, synchronously but briefly)."""
    with _pool_lock:
        pooled = list(_pool.values())
        _pool.clear()
    for entry in pooled:
        with contextlib.suppress(Exception):
            entry.socket.close()


atexit.register(close_all)


def _release(scan_id: str) -> None:
    with _pool_lock:
        pooled = _pool.pop(scan_id, None)
    if pooled is not None:
        _close_quietly(pooled.socket)


# ---------------------------------------------------------------------------
# Scan state and arguments
# ---------------------------------------------------------------------------


@dataclass(kw_only=True)
class JetstreamState(ArrowSerializableDataclass):
    """Where a Jetstream scan has got to, persisted between ticks.

    Everything needed to resume lives here, not in the connection: the pooled
    socket is an optimisation, and a tick that finds none — the scan moved to
    another process, or the socket dropped — reconnects from ``cursor`` on the
    same ``host`` and loses nothing.
    """

    scan_id: str = ""
    host: str = ""
    #: ``time_us`` of the last event read; resume strictly after it.
    cursor: int = 0
    #: ``time.time()`` when the first tick ran; the ``seconds`` budget counts from here.
    started_at: float = 0.0
    events: int = 0
    done: bool = False
    #: Collections, DIDs, start cursor and stop bound, frozen on the first tick
    #: so every reconnect asks for the same stream.
    query_json: str = ""


def _csv(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


@dataclass(slots=True, frozen=True, kw_only=True)
class JetstreamArgs:
    """``jetstream()`` — every argument is named and optional."""

    collections: Annotated[
        str,
        Arg(
            "collections",
            doc="Comma-separated record types to keep, e.g. 'app.bsky.feed.post,app.bsky.feed.like'; "
            "a trailing '.*' matches a prefix, e.g. 'app.bsky.graph.*' (empty = every collection)",
            default="",
        ),
    ] = ""
    dids: Annotated[
        str, Arg("dids", doc="Comma-separated account DIDs to keep (empty = every account)", default="")
    ] = ""
    seconds: Annotated[
        int,
        Arg(
            "seconds",
            doc="How long to listen, wall-clock, before the scan ends (0 = never: stream "
            "batches until the caller stops asking)",
            default=10,
            ge=0,
        ),
    ] = 10
    max_events: Annotated[
        int, Arg("max_events", doc="Stop after this many events (0 = no cap)", default=0, ge=0)
    ] = 0
    batch_events: Annotated[
        int,
        Arg("batch_events", doc="Close a batch after this many events", default=1000, ge=1, le=100_000),
    ] = 1000
    batch_ms: Annotated[
        int,
        Arg(
            "batch_ms",
            doc="Close a batch after this many milliseconds, even if it is short or empty",
            default=1000,
            ge=50,
            le=60_000,
        ),
    ] = 1000
    cursor: Annotated[
        int,
        Arg(
            "cursor",
            doc="Resume after this time_us: pass the highest time_us already read, or "
            "epoch_us(now() - INTERVAL 10 MINUTE) to replay (0 = start live, now)",
            default=0,
            ge=0,
        ),
    ] = 0


def _pushed_time_bounds(params: ProcessParams[Any]) -> tuple[int | None, int | None]:
    """``(start, stop)`` in microseconds from range predicates on ``event_time`` or ``time_us``.

    A lower bound only ever skips events the predicate would drop, so it becomes
    the replay cursor. An upper bound becomes the stopping rule: ``time_us`` is
    strictly increasing, so once one event is past it every later one is too.
    Both columns are the same clock, so their bounds are simply intersected.
    """
    filters = getattr(params, "current_pushdown_filters", None)
    if filters is None:
        return None, None
    lows: list[int] = []
    highs: list[int] = []
    for column in ("event_time", "time_us"):
        bounds = filters.get_column_bounds(column)
        if bounds is None:
            continue
        for scalar, into in (
            (getattr(bounds, "min_value", None), lows),
            (getattr(bounds, "max_value", None), highs),
        ):
            value = scalar.as_py() if scalar is not None else None
            if hasattr(value, "timestamp"):
                into.append(int(value.timestamp() * 1_000_000))
            elif isinstance(value, int) and not isinstance(value, bool):
                into.append(value)
    return (max(lows) if lows else None), (min(highs) if highs else None)


def _initial_query(params: ProcessParams[JetstreamArgs]) -> dict[str, Any]:
    """The stream this scan subscribes to, from its arguments and any pushed WHERE."""
    args = params.args
    collections = _csv(args.collections)
    if not collections and (pushed := equality(params, "collection")):
        collections = [pushed]
    dids = _csv(args.dids)
    if not dids and (pushed := equality(params, "did")):
        dids = [pushed]
    if len(collections) > _MAX_COLLECTIONS:
        raise ValueError(f"Jetstream accepts at most {_MAX_COLLECTIONS} collections, got {len(collections)}")
    if len(dids) > _MAX_DIDS:
        raise ValueError(f"Jetstream accepts at most {_MAX_DIDS} DIDs, got {len(dids)}")
    low, high = _pushed_time_bounds(params)
    # Jetstream's cursor is inclusive — an event's own time_us returns that
    # event first (verified live) — so `cursor` is taken as "after": feeding
    # back the last time_us read resumes with the next event, not a duplicate.
    # For a timestamp, that shifts the start by one microsecond.
    # A pushed lower bound is kept inclusive, since `>=` and `>` look alike here
    # and the predicate itself removes an equal event.
    start = max(args.cursor + 1 if args.cursor else 0, low or 0)
    return {"collections": collections, "dids": dids, "start": start, "stop": high or 0}


def _touch(scan_id: str) -> None:
    """Mark a scan's connection as in use, at the end of a tick as well as the start."""
    with _pool_lock:
        if (pooled := _pool.get(scan_id)) is not None:
            pooled.last_used = time.monotonic()


def _socket_for(state: JetstreamState, query: dict[str, Any], batch_seconds: float) -> ClientConnection:
    """The scan's pooled socket, or a fresh one resuming exactly where the scan left off."""
    now = time.monotonic()
    with _pool_lock:
        _reap(now)
        pooled = _pool.get(state.scan_id)
        if pooled is not None:
            pooled.last_used = now
            return pooled.socket
    resume = state.cursor + 1 if state.cursor else query["start"]
    # Failover is only safe for a live read. A cursor is a point on one
    # instance's clock, so resuming it on another could skip or repeat events;
    # failing loudly is better than a poll that is silently not gapless.
    if state.host:
        candidates: tuple[str, ...] = (state.host,)
    elif query["start"]:
        candidates = hosts()[:1]
    else:
        candidates = hosts()
    failure: Exception | None = None
    # Retried with backoff before giving up: a scan left running for hours will
    # see Jetstream restart or a network blip, and that should cost a pause —
    # resumed losslessly from the cursor — not the whole query.
    for attempt in range(_CONNECT_ATTEMPTS):
        if attempt:
            time.sleep(_CONNECT_BACKOFF_SECONDS * (2 ** (attempt - 1)))
        for host in candidates:
            url = subscribe_url(host, collections=query["collections"], dids=query["dids"], cursor=resume)
            try:
                # `legacy=True` is websockets' supported form for a connection that
                # outlives a `with` block — this one lives in the pool across ticks.
                socket = connect(
                    url, open_timeout=_OPEN_TIMEOUT, close_timeout=_CLOSE_TIMEOUT, max_size=None, legacy=True
                )
            except (OSError, TimeoutError, ConnectionClosed) as exc:
                failure = exc
                continue
            state.host = host
            with _pool_lock:
                _pool[state.scan_id] = _Pooled(
                    socket, url, time.monotonic(), idle_limit=2 * batch_seconds + _IDLE_GRACE_SECONDS
                )
            return socket
    raise ConnectionError(f"could not connect to Jetstream ({', '.join(candidates)}): {failure}")


JETSTREAM_DOCS = docs(
    category="firehose",
    result_schema=JETSTREAM_SCHEMA,
    llm=(
        "Everything happening on Bluesky right now — every post, like, repost, follow, block and "
        "profile change across the whole network — read live from Jetstream, Bluesky's JSON "
        "firehose. Reach for this for network-wide activity that no lookup can answer: what is "
        "being posted this minute, posting volume by language, which hashtags are spiking, the "
        "like rate. It listens for `seconds` (default 10) and ends, so every result is a time-"
        "boxed sample of a stream about 400 events a second strong. Filter with `collections`, "
        "or replay a recent window with `cursor` or a WHERE on `event_time`."
    ),
    md=(
        "A bounded read of Bluesky's firehose, via Jetstream.\n\n"
        "### What an event is\n\n"
        "Nearly every row is a **commit**: a record created, updated or deleted in someone's "
        "repository. `collection` says what kind — `app.bsky.feed.post`, `app.bsky.feed.like`, "
        "`app.bsky.feed.repost`, `app.bsky.graph.follow`, `app.bsky.graph.block`, "
        "`app.bsky.actor.profile` and more. The whole record is in `record` as JSON; the post "
        "columns (`text`, `langs`, `hashtags`, …) are filled for posts, and `subject_uri` / "
        "`subject_did` for likes, reposts, follows and blocks. A few rows are `identity` or "
        "`account` events, which arrive regardless of any collection filter.\n\n"
        "Events are raw: there are no author handles and no like counts here. Join a post's `uri` "
        "to `post()` or a `did` to `profile()` under a LATERAL to hydrate them.\n\n"
        "### How long it runs\n\n"
        "It stops at the first of: `seconds` of wall-clock listening (default 10), `max_events`, "
        "a `LIMIT`, or an upper bound on `event_time` / `time_us` in the WHERE clause. Rows arrive "
        "about once a second while it listens.\n\n"
        "### Running without an end\n\n"
        "`seconds => 0` streams until the query is cancelled, one batch per `batch_ms`. DuckDB "
        "buffers about 1 MB of a streaming result (`streaming_buffer_size`) before handing it to "
        "the client, which a firehose of small rows is slow to fill — run `SET "
        "streaming_buffer_size = '1KB'` first so rows reach you as they arrive.\n\n"
        "### Replaying the recent past\n\n"
        "Jetstream buffers roughly the last day and a half. Pass `cursor => "
        "epoch_us(now() - INTERVAL 10 MINUTE)`, or simply `WHERE event_time >= ...`, to start "
        "there; a range such as `WHERE event_time BETWEEN now() - INTERVAL 2 MINUTE AND now() - "
        "INTERVAL 1 MINUTE` replays exactly that minute and stops. Replay runs far faster than "
        "real time, but a long window still needs a larger `seconds`. A cursor older than the "
        "buffer starts at the oldest event without an error.\n\n"
        "### Filters that are pushed to Jetstream\n\n"
        "`WHERE collection = '...'` and `WHERE did = '...'` become Jetstream's own filters, so "
        "the unwanted events never cross the network. `collections` also takes a prefix "
        "wildcard, e.g. `'app.bsky.graph.*'`.\n\n"
        "### Polling continuously\n\n"
        "`time_us` is the cursor. Remember the highest `time_us` you have read and pass it back "
        "as `cursor`: the next call resumes with the event after it, so successive calls neither "
        "skip nor repeat anything. Each call returns up to `max_events` events or `seconds` of "
        "listening, whichever comes first, so `cursor => <last>, max_events => 1000` is one "
        "page of the stream. A call that finds nothing new returns no rows; keep the old cursor.\n\n"
        "Prefer `cursor =>` over `WHERE time_us > <last>` for this: `max_events` counts events "
        "read *before* any WHERE is applied, so the WHERE form spends one of them on the event "
        "you already have. Cursors are points on one Jetstream instance's clock, so a call with "
        "a cursor never fails over to another instance; set `BLUESKY_JETSTREAM_URL` to choose "
        "which one."
    ),
    example_queries=examples(
        (
            "Ten seconds of new posts from across the whole network",
            "SELECT event_time, did, text, langs FROM bluesky.main.jetstream("
            "collections => 'app.bsky.feed.post') WHERE operation = 'create'",
        ),
        (
            "What the network is doing right now, by record type",
            "SELECT collection, operation, count(*) AS events FROM bluesky.main.jetstream(seconds => 5) "
            "GROUP BY ALL ORDER BY events DESC",
        ),
        (
            "Posting volume by language over the last minute, replayed",
            "SELECT lang, count(*) AS posts FROM (SELECT unnest(langs) AS lang "
            "FROM bluesky.main.jetstream(collections => 'app.bsky.feed.post', seconds => 60) "
            "WHERE event_time >= now() - INTERVAL 1 MINUTE AND event_time < now()) "
            "GROUP BY lang ORDER BY posts DESC LIMIT 10",
        ),
        (
            "Hashtags trending in the live stream",
            "SELECT tag, count(*) AS uses FROM (SELECT unnest(hashtags) AS tag FROM "
            "bluesky.main.jetstream(collections => 'app.bsky.feed.post', seconds => 20)) "
            "GROUP BY tag ORDER BY uses DESC LIMIT 15",
        ),
        (
            "The most-liked posts of the last 30 seconds, hydrated with post()",
            "SELECT p.author_handle, p.text, j.likes FROM (SELECT subject_uri, count(*) AS likes "
            "FROM bluesky.main.jetstream(collections => 'app.bsky.feed.like', seconds => 30) "
            "WHERE subject_uri LIKE '%/app.bsky.feed.post/%' GROUP BY subject_uri "
            "ORDER BY likes DESC LIMIT 5) j, LATERAL bluesky.main.post(j.subject_uri) p "
            "ORDER BY j.likes DESC",
        ),
    ),
)


@init_single_worker
class JetstreamFunction(TableFunctionGenerator[JetstreamArgs, JetstreamState]):
    """A time-boxed read of the Jetstream firehose, one second of events per tick."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = JETSTREAM_SCHEMA

    class Meta:
        name = "jetstream"
        description = "Bluesky's live firehose via Jetstream: every post, like, follow and more, time-boxed"
        categories = ["firehose"]
        projection_pushdown = True
        #: `collection` / `did` equality become Jetstream's own filters, and
        #: event_time / time_us bounds become the start cursor and stop rule.
        #: Not exact: every predicate is still applied to the rows.
        filter_pushdown = True
        auto_apply_filters = True
        tags = JETSTREAM_DOCS
        examples = [
            FunctionExample(
                sql=(
                    "SELECT collection, operation, count(*) AS events "
                    "FROM bluesky.main.jetstream(seconds => 5) GROUP BY ALL ORDER BY events DESC"
                ),
                description="What the network is doing right now, by record type",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[JetstreamArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[JetstreamArgs]) -> JetstreamState:
        return JetstreamState(scan_id=uuid.uuid4().hex)

    @classmethod
    def process(
        cls, params: ProcessParams[JetstreamArgs], state: JetstreamState, out: OutputCollector
    ) -> None:
        """Read one batch — ``batch_events`` events or ``batch_ms`` — emit it, and record where to resume.

        When DuckDB stops asking — a LIMIT met, a query cancelled — the scan is
        simply never called again; cancellation lands within one ``batch_ms``,
        and the abandoned socket is closed by the reaper.
        """
        if state.done:
            _release(state.scan_id)
            out.finish()
            return
        if not state.query_json:
            state.query_json = json.dumps(_initial_query(params), sort_keys=True)
            state.started_at = time.time()
        query = json.loads(state.query_json)
        args = params.args
        deadline = state.started_at + args.seconds if args.seconds else float("inf")
        stop_us = int(query["stop"])
        cap = args.max_events

        socket = _socket_for(state, query, args.batch_ms / 1000)
        rows: list[dict[str, Any]] = []
        tick_end = min(time.time() + args.batch_ms / 1000, deadline)
        while len(rows) < args.batch_events:
            remaining = tick_end - time.time()
            if remaining <= 0:
                break
            try:
                message = socket.recv(timeout=remaining)
            except TimeoutError:
                break
            except ConnectionClosed:
                # Drop the dead socket; the next tick reconnects from the cursor.
                _release(state.scan_id)
                break
            try:
                event = json.loads(message)
            except (ValueError, TypeError):
                continue
            if not isinstance(event, dict):
                continue
            time_us = event.get("time_us")
            if not isinstance(time_us, int) or time_us <= state.cursor:
                continue  # a replayed overlap after a reconnect
            if stop_us and time_us > stop_us:
                state.done = True
                break
            rows.append(flatten_jetstream_event(event))
            state.cursor = time_us
            state.events += 1
            if cap and state.events >= cap:
                state.done = True
                break
        if time.time() >= deadline:
            state.done = True
        if state.done:
            _release(state.scan_id)
        else:
            _touch(state.scan_id)
        batch, _ = build_filtered(params, rows)
        out.emit(batch)


JETSTREAM_FUNCTIONS: list[type] = [JetstreamFunction]
