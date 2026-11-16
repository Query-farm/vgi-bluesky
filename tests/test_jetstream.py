"""Jetstream: event flattening, pushdown translation, and the tick loop against a fake socket."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
import pytest
from websockets.exceptions import ConnectionClosedError

from vgi_bluesky import jetstream
from vgi_bluesky.jetstream import JetstreamArgs, JetstreamFunction, JetstreamState, subscribe_url
from vgi_bluesky.schemas import JETSTREAM_SCHEMA, batch_from_rows, flatten_jetstream_event

DID = "did:plc:abc"


def _commit(time_us: int, collection: str = "app.bsky.feed.post", **record: Any) -> dict[str, Any]:
    return {
        "did": DID,
        "time_us": time_us,
        "kind": "commit",
        "commit": {
            "rev": "r1",
            "operation": "create",
            "collection": collection,
            "rkey": f"k{time_us}",
            "cid": "bafy",
            "record": {"$type": collection, "createdAt": "2026-09-27T21:46:07.538Z", **record},
        },
    }


class TestFlatten:
    def test_post(self) -> None:
        row = flatten_jetstream_event(
            _commit(1_790_545_568_613_367, text="hi #duckdb", langs=["en"], tags=["sql"])
        )
        assert row["uri"] == f"at://{DID}/app.bsky.feed.post/k1790545568613367"
        assert row["event_time"] == datetime.fromtimestamp(1_790_545_568.613367, tz=UTC)
        assert (row["text"], row["langs"], row["hashtags"]) == ("hi #duckdb", ["en"], ["sql"])
        assert json.loads(row["record"])["text"] == "hi #duckdb"

    def test_like_subject(self) -> None:
        subject = {"uri": "at://did:plc:author/app.bsky.feed.post/3x", "cid": "c"}
        row = flatten_jetstream_event(_commit(1, "app.bsky.feed.like", subject=subject))
        assert row["subject_uri"] == "at://did:plc:author/app.bsky.feed.post/3x"
        assert row["subject_did"] == "did:plc:author"
        assert row["text"] is None  # post columns are only for posts

    def test_follow_subject_is_a_did(self) -> None:
        row = flatten_jetstream_event(_commit(1, "app.bsky.graph.follow", subject="did:plc:followed"))
        assert row["subject_did"] == "did:plc:followed" and row["subject_uri"] is None

    def test_delete_has_no_record(self) -> None:
        event = {
            "did": DID,
            "time_us": 5,
            "kind": "commit",
            "commit": {"rev": "r", "operation": "delete", "collection": "app.bsky.feed.like", "rkey": "k"},
        }
        row = flatten_jetstream_event(event)
        assert row["operation"] == "delete" and row["record"] is None and row["uri"] is not None

    def test_identity_and_account(self) -> None:
        identity = flatten_jetstream_event(
            {"did": DID, "time_us": 1, "kind": "identity", "identity": {"handle": "a.test"}}
        )
        account = flatten_jetstream_event(
            {"did": DID, "time_us": 2, "kind": "account", "account": {"active": False, "status": "takendown"}}
        )
        assert identity["handle"] == "a.test" and identity["collection"] is None
        assert (account["active"], account["account_status"]) == (False, "takendown")

    def test_malformed_events_still_build(self) -> None:
        rows = [
            flatten_jetstream_event({"kind": "commit", "commit": "nonsense"}),
            flatten_jetstream_event({"time_us": "soon", "commit": {"record": ["not", "a", "dict"]}}),
            flatten_jetstream_event({}),
        ]
        assert batch_from_rows(rows, JETSTREAM_SCHEMA).num_rows == 3


class TestSubscribeUrl:
    def test_filters_repeat_and_cursor_is_optional(self) -> None:
        url = subscribe_url(
            "wss://j.test", collections=["app.bsky.feed.post", "app.bsky.graph.*"], dids=[DID], cursor=0
        )
        assert url == (
            "wss://j.test/subscribe?wantedCollections=app.bsky.feed.post"
            "&wantedCollections=app.bsky.graph.%2A&wantedDids=did%3Aplc%3Aabc"
        )

    def test_no_filters(self) -> None:
        assert (
            subscribe_url("wss://j.test", collections=[], dids=[], cursor=42)
            == "wss://j.test/subscribe?cursor=42"
        )

    def test_env_pins_one_host(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BLUESKY_JETSTREAM_URL", "wss://mine.test/")
        assert jetstream.hosts() == ("wss://mine.test",)


# ---------------------------------------------------------------------------
# Pushdown
# ---------------------------------------------------------------------------


@dataclass
class _Bounds:
    min_value: Any = None
    max_value: Any = None


class _Filters:
    def __init__(
        self, constants: dict[str, str] | None = None, bounds: dict[str, tuple[Any, Any]] | None = None
    ):
        self._constants = constants or {}
        self._bounds = bounds or {}

    def get_column_constant(self, column: str) -> Any:
        value = self._constants.get(column)
        return pa.scalar(value) if value is not None else None

    def get_column_bounds(self, column: str) -> _Bounds | None:
        if column not in self._bounds:
            return None
        low, high = self._bounds[column]
        return _Bounds(
            pa.scalar(low) if low is not None else None, pa.scalar(high) if high is not None else None
        )

    def evaluate(self, batch: pa.RecordBatch) -> pa.BooleanArray:
        return pa.array([True] * batch.num_rows)


@dataclass
class _Params:
    args: JetstreamArgs = field(default_factory=lambda: JetstreamArgs(batch_ms=50))
    current_pushdown_filters: Any = None
    output_schema: pa.Schema = JETSTREAM_SCHEMA


T0 = datetime(2026, 9, 27, 21, 0, tzinfo=UTC)
T1 = datetime(2026, 9, 27, 21, 1, tzinfo=UTC)
US0, US1 = int(T0.timestamp() * 1e6), int(T1.timestamp() * 1e6)


class TestInitialQuery:
    def test_equalities_become_jetstream_filters(self) -> None:
        params = _Params(current_pushdown_filters=_Filters({"collection": "app.bsky.feed.like", "did": DID}))
        query = jetstream._initial_query(params)  # type: ignore[arg-type]
        assert query["collections"] == ["app.bsky.feed.like"] and query["dids"] == [DID]

    def test_explicit_arguments_win(self) -> None:
        params = _Params(
            args=JetstreamArgs(collections="app.bsky.feed.post, app.bsky.feed.repost"),
            current_pushdown_filters=_Filters({"collection": "app.bsky.feed.like"}),
        )
        query = jetstream._initial_query(params)  # type: ignore[arg-type]
        assert query["collections"] == ["app.bsky.feed.post", "app.bsky.feed.repost"]

    def test_time_bounds_become_cursor_and_stop(self) -> None:
        params = _Params(current_pushdown_filters=_Filters(bounds={"event_time": (T0, T1)}))
        query = jetstream._initial_query(params)  # type: ignore[arg-type]
        assert (query["start"], query["stop"]) == (US0, US1)

    def test_bounds_on_both_clocks_intersect(self) -> None:
        params = _Params(
            args=JetstreamArgs(cursor=US0 - 5),  # resumes at US0 - 4
            current_pushdown_filters=_Filters(bounds={"event_time": (T0, T1), "time_us": (US0 + 7, US1 + 9)}),
        )
        query = jetstream._initial_query(params)  # type: ignore[arg-type]
        assert (query["start"], query["stop"]) == (US0 + 7, US1)

    def test_cursor_resumes_after_the_event_it_names(self) -> None:
        """Jetstream's cursor is inclusive; feeding back the last time_us must not repeat it."""
        query = jetstream._initial_query(_Params(args=JetstreamArgs(cursor=US0, batch_ms=50)))  # type: ignore[arg-type]
        assert query["start"] == US0 + 1

    def test_too_many_collections_is_an_error(self) -> None:
        params = _Params(args=JetstreamArgs(collections=",".join(f"c{i}" for i in range(101))))
        with pytest.raises(jetstream.InvalidArgumentError, match="at most 100") as info:
            jetstream._initial_query(params)  # type: ignore[arg-type]
        assert info.value.error_code == "INVALID_ARGUMENT" and info.value.field == "collections"


# ---------------------------------------------------------------------------
# The tick loop
#: The ops of one multi-op commit, as Jetstream emits them: one event each, one time_us.
COMMIT_OPS = ["app.bsky.feed.post", "app.bsky.feed.threadgate", "app.bsky.feed.postgate"]


def _ops(time_us: int) -> list[dict[str, Any]]:
    return [_commit(time_us, collection) for collection in COMMIT_OPS]


# ---------------------------------------------------------------------------


class _FakeSocket:
    """Serves queued messages, then times out; can be told to drop mid-stream."""

    def __init__(self, messages: list[Any], drop_after: int | None = None) -> None:
        self.messages = [m if isinstance(m, str) else json.dumps(m) for m in messages]
        self.drop_after = drop_after
        self.served = 0
        self.closed = False

    def recv(self, timeout: float | None = None) -> str:
        if self.drop_after is not None and self.served >= self.drop_after:
            raise ConnectionClosedError(None, None)
        if not self.messages:
            raise TimeoutError
        self.served += 1
        return self.messages.pop(0)

    def close(self) -> None:
        self.closed = True


@dataclass
class _Out:
    batches: list[pa.RecordBatch] = field(default_factory=list)
    finished: bool = False

    def emit(self, batch: pa.RecordBatch, **_: Any) -> None:
        self.batches.append(batch)

    def finish(self) -> None:
        self.finished = True


class _Sockets(list[tuple[str, _FakeSocket]]):
    """Every connect() call as (url, socket), answered from ``queue``."""

    def __init__(self) -> None:
        super().__init__()
        self.queue: list[_FakeSocket] = []


@pytest.fixture
def sockets(monkeypatch: pytest.MonkeyPatch) -> _Sockets:
    opened = _Sockets()

    def fake_connect(url: str, **_: Any) -> _FakeSocket:
        socket = opened.queue.pop(0)
        opened.append((url, socket))
        return socket

    monkeypatch.setattr(jetstream, "connect", fake_connect)
    monkeypatch.setattr(jetstream.time, "sleep", lambda _s: None)
    monkeypatch.setattr(jetstream, "_close_quietly", lambda socket: socket.close())
    return opened


class _Clock:
    """``time.time()`` for the module under test, advanced only by the fakes."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.now = 0.0
        monkeypatch.setattr(jetstream.time, "time", lambda: self.now)


class _ClockedSocket(_FakeSocket):
    """A socket whose idle waits take (fake) time, as a real recv(timeout) does."""

    def __init__(self, clock: _Clock, messages: list[Any]) -> None:
        super().__init__(messages)
        self.clock = clock

    def recv(self, timeout: float | None = None) -> str:
        if not self.messages:
            self.clock.now += timeout or 0.0
        return super().recv(timeout)


def _slow_connect(clock: _Clock, sockets: _Sockets, seconds: float) -> Any:
    def connect(url: str, **_: Any) -> _FakeSocket:
        clock.now += seconds
        socket = sockets.queue.pop(0)
        sockets.append((url, socket))
        return socket

    return connect


def _run(params: _Params, ticks: int = 10) -> tuple[_Out, JetstreamState]:
    state = JetstreamFunction.initial_state(params)  # type: ignore[arg-type]
    out = _Out()
    for _ in range(ticks):
        if out.finished:
            break
        JetstreamFunction.process(params, state, out)  # type: ignore[arg-type]
    return out, state


def _times(out: _Out) -> list[int]:
    return [t for b in out.batches for t in b.column("time_us").to_pylist()]


class TestTickLoop:
    def test_max_events_stops_the_scan(self, sockets: _Sockets) -> None:
        sockets.queue.append(_FakeSocket([_commit(t) for t in range(1, 50)]))
        out, state = _run(_Params(args=JetstreamArgs(max_events=5, batch_ms=50)))
        assert _times(out) == [1, 2, 3, 4, 5]
        assert state.done and out.finished and sockets[0][1].closed

    def test_an_upper_time_bound_stops_without_including_the_overshoot(self, sockets: _Sockets) -> None:
        sockets.queue.append(_FakeSocket([_commit(t) for t in (US0, US0 + 1, US1, US1 + 1, US1 + 2)]))
        params = _Params(current_pushdown_filters=_Filters(bounds={"time_us": (None, US1)}))
        out, state = _run(params)
        assert _times(out) == [US0, US0 + 1, US1] and state.done

    def test_a_reconnect_resumes_at_the_cursor_and_drops_the_overlap(self, sockets: _Sockets) -> None:
        sockets.queue.append(_FakeSocket([_commit(t) for t in (10, 11, 12)], drop_after=3))
        # Jetstream's cursor is inclusive, so the new socket repeats 12 before moving on.
        sockets.queue.append(_FakeSocket([_commit(t) for t in (12, 13, 14)]))
        out, _state = _run(_Params(args=JetstreamArgs(max_events=5, batch_ms=50)))
        assert _times(out) == [10, 11, 12, 13, 14]
        assert "cursor=12" in sockets[1][0]

    def test_every_op_of_a_commit_is_kept(self, sockets: _Sockets) -> None:
        """The ops of one commit share a time_us; none of them is a duplicate."""
        sockets.queue.append(_FakeSocket([_commit(1), *_ops(2), _commit(3)]))
        out, _state = _run(_Params(args=JetstreamArgs(max_events=5, batch_ms=50)))
        assert _times(out) == [1, 2, 2, 2, 3]

    def test_a_reconnect_inside_a_commit_loses_and_repeats_nothing(self, sockets: _Sockets) -> None:
        sockets.queue.append(_FakeSocket([_commit(1), *_ops(2)[:2]], drop_after=3))
        sockets.queue.append(_FakeSocket([*_ops(2), _commit(3)]))
        out, _state = _run(_Params(args=JetstreamArgs(max_events=5, batch_ms=50)))
        assert _times(out) == [1, 2, 2, 2, 3]
        assert [r["collection"] for b in out.batches for r in b.to_pylist()][1:4] == COMMIT_OPS
        assert "cursor=2" in sockets[1][0]

    def test_max_events_finishes_the_commit_it_stopped_in(self, sockets: _Sockets) -> None:
        """So the last time_us is a cursor that resumes without skipping the commit's other ops."""
        sockets.queue.append(_FakeSocket([_commit(1), *_ops(2), _commit(3)]))
        out, state = _run(_Params(args=JetstreamArgs(max_events=2, batch_ms=50)))
        assert _times(out) == [1, 2, 2, 2] and state.done and state.cursor == 2

    def test_an_upper_time_bound_keeps_every_op_at_the_bound(self, sockets: _Sockets) -> None:
        sockets.queue.append(_FakeSocket([_commit(US0), *_ops(US1), _commit(US1 + 1)]))
        params = _Params(current_pushdown_filters=_Filters(bounds={"time_us": (None, US1)}))
        out, state = _run(params)
        assert _times(out) == [US0, US1, US1, US1] and state.done

    def test_batches_close_at_batch_events(self, sockets: _Sockets) -> None:
        """One socket serves every tick, and each batch holds at most batch_events."""
        sockets.queue.append(_FakeSocket([_commit(t) for t in range(1, 7)]))
        out, _state = _run(_Params(args=JetstreamArgs(max_events=6, batch_events=2, batch_ms=50)))
        assert [b.num_rows for b in out.batches] == [2, 2, 2] and len(sockets) == 1

    def test_garbage_messages_are_skipped(self, sockets: _Sockets) -> None:
        sockets.queue.append(_FakeSocket(["not json", "[1, 2]", _commit(1), {"time_us": "x"}, _commit(2)]))
        out, _state = _run(_Params(args=JetstreamArgs(max_events=2, batch_ms=50)))
        assert _times(out) == [1, 2]

    def test_the_wall_clock_budget_ends_a_quiet_stream(
        self, sockets: _Sockets, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sockets.queue.append(_FakeSocket([]))
        clock = iter(range(0, 1000, 5))
        monkeypatch.setattr(jetstream.time, "time", lambda: float(next(clock)))
        out, state = _run(_Params(args=JetstreamArgs(seconds=1, batch_ms=50)))
        assert state.done and out.finished and _times(out) == []

    def test_the_budget_starts_once_the_connection_is_open(
        self, sockets: _Sockets, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A 5-second handshake does not spend a 3-second listen."""
        clock = _Clock(monkeypatch)
        sockets.queue.append(_ClockedSocket(clock, [_commit(1), _commit(2)]))
        monkeypatch.setattr(jetstream, "connect", _slow_connect(clock, sockets, seconds=5))
        out, state = _run(_Params(args=JetstreamArgs(seconds=3, batch_ms=500)), ticks=50)
        assert _times(out) == [1, 2] and state.done and out.finished
        assert state.started_at == 5.0
        assert clock.now <= 5.0 + 3 + 1, "the listen itself still ends on budget"

    def test_a_dead_instance_costs_one_connect_timeout_not_the_budget(
        self, sockets: _Sockets, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A live read fails over after one bounded attempt and still listens for its full budget."""
        clock = _Clock(monkeypatch)
        attempts: list[str] = []

        def first_host_hangs(url: str, open_timeout: float, **_: Any) -> Any:
            attempts.append(url)
            if len(attempts) == 1:
                clock.now += open_timeout
                raise TimeoutError("timed out during opening handshake")
            return _ClockedSocket(clock, [_commit(1)])

        monkeypatch.setattr(jetstream, "connect", first_host_hangs)
        out, state = _run(_Params(args=JetstreamArgs(seconds=3, batch_ms=500)), ticks=50)
        assert _times(out) == [1] and state.done
        assert [u.split("/subscribe")[0] for u in attempts] == list(jetstream.hosts()[:2])
        assert state.started_at == jetstream._OPEN_TIMEOUT

    def test_the_pushed_start_is_the_first_cursor(self, sockets: _Sockets) -> None:
        sockets.queue.append(_FakeSocket([_commit(US0 + 1)]))
        params = _Params(
            args=JetstreamArgs(max_events=1, batch_ms=50),
            current_pushdown_filters=_Filters(bounds={"event_time": (T0, None)}),
        )
        _run(params)
        assert f"cursor={US0}" in sockets[0][0]

    def test_a_cursor_never_fails_over(self, sockets: _Sockets, monkeypatch: pytest.MonkeyPatch) -> None:
        """A cursor is one instance's clock; resuming it elsewhere would not be gapless."""

        def refuse(url: str, **_: Any) -> Any:
            sockets.append((url, _FakeSocket([])))
            raise OSError("down")

        monkeypatch.setattr(jetstream, "connect", refuse)
        with pytest.raises(jetstream.JetstreamUnavailableError) as info:
            _run(_Params(args=JetstreamArgs(cursor=US0, batch_ms=50)))
        assert info.value.error_code == "UNAVAILABLE"
        assert len(sockets) == jetstream._CONNECT_ATTEMPTS
        assert {url.split("/subscribe")[0] for url, _s in sockets} == {jetstream.hosts()[0]}

    def test_a_live_read_does_fail_over(self, sockets: _Sockets, monkeypatch: pytest.MonkeyPatch) -> None:
        attempts: list[str] = []

        def flaky(url: str, **_: Any) -> Any:
            attempts.append(url)
            if len(attempts) == 1:
                raise OSError("down")
            return _FakeSocket([_commit(1)])

        monkeypatch.setattr(jetstream, "connect", flaky)
        out, _state = _run(_Params(args=JetstreamArgs(max_events=1, batch_ms=50)))
        assert _times(out) == [1] and len(attempts) == 2

    def test_seconds_zero_never_ends_on_its_own(self, sockets: _Sockets) -> None:
        """Endless mode: every tick emits a batch, even an empty one, until the caller stops asking."""
        sockets.queue.append(_FakeSocket([_commit(1)]))
        out, state = _run(_Params(args=JetstreamArgs(seconds=0, batch_ms=50)), ticks=10)
        assert not state.done and not out.finished
        assert len(out.batches) == 10 and _times(out) == [1]

    def test_a_transient_outage_is_retried_on_the_same_host(
        self, sockets: _Sockets, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        attempts: list[str] = []

        def blip(url: str, **_: Any) -> Any:
            attempts.append(url)
            if len(attempts) < 3:
                raise OSError("restarting")
            return _FakeSocket([_commit(7)])

        monkeypatch.setattr(jetstream, "connect", blip)
        out, _state = _run(_Params(args=JetstreamArgs(cursor=5, max_events=1, batch_ms=50)))
        assert _times(out) == [7] and len(attempts) == 3


class TestLiveReader:
    """The live tests' own Jetstream reader fails over, so one unreachable instance is not a red run."""

    def test_moves_on_from_an_instance_that_will_not_open(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import jetstream_live

        tried: list[str] = []

        def connect(url: str, **_: Any) -> Any:
            tried.append(url)
            if len(tried) == 1:
                raise TimeoutError("timed out during opening handshake")
            return _ContextSocket([_commit(1), _commit(2)])

        monkeypatch.setattr(jetstream_live, "connect", connect)
        host, messages = jetstream_live.read_raw(lambda h: f"{h}/subscribe", 2, hosts=["wss://a", "wss://b"])
        assert host == "wss://b" and tried == ["wss://a/subscribe", "wss://b/subscribe"]
        assert [json.loads(m)["time_us"] for m in messages] == [1, 2]

    def test_gives_up_after_two_rounds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import jetstream_live

        tried: list[str] = []

        def refuse(url: str, **_: Any) -> Any:
            tried.append(url)
            raise OSError("unreachable")

        monkeypatch.setattr(jetstream_live, "connect", refuse)
        with pytest.raises(ConnectionError, match="unreachable"):
            jetstream_live.read_raw(lambda h: h, 1, hosts=["wss://a", "wss://b"])
        assert tried == ["wss://a", "wss://b"] * jetstream_live.ROUNDS


class _ContextSocket(_FakeSocket):
    def __enter__(self) -> _ContextSocket:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
