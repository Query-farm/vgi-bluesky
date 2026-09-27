"""Cursor paging as scan state: one page per tick, a frozen query, and a clean finish."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
import pytest

from vgi_bluesky import paging
from vgi_bluesky.paging import PagedScanState, emit_page
from vgi_bluesky.schemas import ACTOR_SCHEMA, flatten_actor


@dataclass
class _Params:
    output_schema: pa.Schema = ACTOR_SCHEMA
    current_pushdown_filters: Any = None


@dataclass
class _Out:
    batches: list[pa.RecordBatch] = field(default_factory=list)
    cache: list[Any] = field(default_factory=list)
    finished: bool = False

    def emit(self, batch: pa.RecordBatch, cache_control: Any = None) -> None:
        self.batches.append(batch)
        self.cache.append(cache_control)

    def finish(self) -> None:
        self.finished = True


class _FakeApi:
    """Serves a fixed sequence of pages and records the queries it was asked."""

    def __init__(self, pages: list[tuple[list[dict[str, Any]], str | None]]) -> None:
        self.pages = pages
        self.calls: list[tuple[dict[str, Any], str | None]] = []

    def page(self, method: str, key: str, query: dict[str, Any], *, cursor: str | None, **_: Any) -> Any:
        self.calls.append((dict(query), cursor))
        rows, next_cursor = self.pages[len(self.calls) - 1]
        return {key: rows}, rows, next_cursor


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> _FakeApi:
    api = _FakeApi(
        [
            ([{"did": "did:plc:a", "handle": "a.test"}], "c1"),
            ([{"did": "did:plc:b", "handle": "b.test"}], "c2"),
            ([{"did": "did:plc:c", "handle": "c.test"}], None),
        ]
    )
    monkeypatch.setattr(paging.api, "page", api.page)
    return api


def _flatten(_payload: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [flatten_actor(r) for r in rows]


def _walk(query: Any, **kwargs: Any) -> tuple[_Out, PagedScanState]:
    state, out = PagedScanState(), _Out()
    for _ in range(10):
        if out.finished:
            break
        emit_page(
            _Params(),  # type: ignore[arg-type]
            state,
            out,  # type: ignore[arg-type]
            method="app.bsky.graph.getFollowers",
            key="followers",
            query=query,
            flatten=_flatten,
            **kwargs,
        )
    return out, state


class TestWalk:
    def test_one_page_per_tick_then_finish(self, fake: _FakeApi) -> None:
        out, state = _walk({"actor": "x"})
        assert [b.num_rows for b in out.batches] == [1, 1, 1]
        assert [cursor for _q, cursor in fake.calls] == [None, "c1", "c2"]
        assert state.done and out.finished

    def test_first_page_only_stops_despite_a_cursor(self, fake: _FakeApi) -> None:
        out, _state = _walk({"q": "x"}, first_page_only=True)
        assert len(out.batches) == 1 and len(fake.calls) == 1 and out.finished


class TestFrozenQuery:
    def test_a_thunk_is_evaluated_once_per_walk(self, fake: _FakeApi) -> None:
        """A query that costs a request to build (handle resolution) is paid once, not per page."""
        built: list[int] = []

        def query() -> dict[str, Any]:
            built.append(1)
            return {"uri": "at://did:plc:x/app.bsky.feed.post/1"}

        _walk(query)
        assert built == [1]
        assert {q["uri"] for q, _c in fake.calls} == {"at://did:plc:x/app.bsky.feed.post/1"}

    def test_a_changed_query_mid_walk_is_ignored(self, fake: _FakeApi) -> None:
        """A cursor is only meaningful against the query that minted it."""
        state, out = PagedScanState(), _Out()
        for actor in ("first", "second", "third"):
            emit_page(
                _Params(),  # type: ignore[arg-type]
                state,
                out,  # type: ignore[arg-type]
                method="app.bsky.graph.getFollowers",
                key="followers",
                query={"actor": actor},
                flatten=_flatten,
            )
        assert [q["actor"] for q, _c in fake.calls] == ["first", "first", "first"]


class TestCacheControl:
    def test_opt_in_ttl_applies_when_the_origin_declares_nothing(self, fake: _FakeApi) -> None:
        out, _ = _walk({"q": "x"}, opt_in_ttl=60, first_page_only=True)
        assert out.cache[0] is not None and out.cache[0].ttl == 60

    def test_nothing_is_cached_by_default(self, fake: _FakeApi) -> None:
        out, _ = _walk({"q": "x"}, first_page_only=True)
        assert out.cache == [None]
