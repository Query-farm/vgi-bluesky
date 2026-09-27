"""Cursor paging as VGI scan state.

Bluesky pages every list endpoint with an opaque cursor. Walking that cursor to
exhaustion inside one call is the obvious implementation and the wrong one: an
account like ``bsky.app`` has tens of millions of followers, so a
``LIMIT 10`` would still pay for every page, and because a scan blocked inside
its first batch cannot be cancelled, the query would wedge the client rather
than merely run slowly.

A ``TableFunctionGenerator`` carries state between ``process()`` ticks, so the
cursor lives in that state and each tick fetches exactly one page and emits
exactly one batch. DuckDB sees rows immediately, a ``LIMIT`` stops the walk
early, and cancellation lands between ticks.

The trade-off: a stateful scan cannot be the inner side of a correlated
``LATERAL``. So the rule this package follows is *if the endpoint pages, the
function pages* — cursor-paged endpoints become scans, while the bounded ones
(a batch of profiles, a batch of posts, one thread) stay blended and composable.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from vgi.cache_control import CacheControl
from vgi.table_function import ProcessParams
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi_bluesky import bluesky_api as api
from vgi_bluesky.bluesky_api import PAGE_LIMIT, STALE_IF_ERROR, CacheHint
from vgi_bluesky.pushdown import build_filtered

#: Turns one page's payload and rows into output rows. Receives the whole
#: payload because some endpoints carry context beside the rows — the
#: ``subject`` of a follower list — that each output row needs.
Flatten = Callable[[dict[str, Any], list[dict[str, Any]]], list[dict[str, Any]]]

#: Query parameters, or a thunk that builds them on the first page only.
Query = dict[str, Any] | Callable[[], dict[str, Any]]


@dataclass(kw_only=True)
class PagedScanState(ArrowSerializableDataclass):
    """Where a cursor-paged scan has got to.

    The framework persists this between ``process()`` ticks. ``cursor`` is
    Bluesky's opaque continuation token; an empty string means "not started",
    which the cursor alone cannot tell apart from "finished" — hence ``done``.
    """

    cursor: str = ""
    done: bool = False
    #: The query the walk started with, frozen as JSON on the first page.
    #: ``current_pushdown_filters`` is refreshed before every tick, so a scan
    #: that recomputed its parameters each time could resume an opaque cursor
    #: under different ones — and the failure would be wrong rows, not an error.
    query_json: str = ""


def _frozen_query(state: PagedScanState, query: Query) -> dict[str, Any]:
    """The query this walk began with, so every page uses the same one.

    ``query`` may be a callable, evaluated only for the first page. That is for
    queries that cost a request to build — resolving a handle in a post URI to
    a DID — which should be paid once per walk, not once per page.
    """
    if state.cursor and state.query_json:
        return dict(json.loads(state.query_json))
    built = query() if callable(query) else query
    state.query_json = json.dumps(built, sort_keys=True, default=str)
    return built


def emit_page(
    params: ProcessParams[Any],
    state: PagedScanState,
    out: OutputCollector,
    *,
    method: str,
    key: str,
    query: Query,
    flatten: Flatten,
    base: str | None = None,
    opt_in_ttl: int = 0,
    first_page_only: bool = False,
) -> None:
    """Fetch one page into one batch, and record where to resume.

    Args:
        params: The tick's parameters; ``output_schema`` is the projected one.
        state: The cursor carried between ticks.
        out: Collector for the batch, or for ``finish()`` once the walk is done.
        method: XRPC method NSID.
        key: Key in the response holding this page's rows.
        query: Query parameters, before the cursor and page size are added, or
            a thunk building them (see :func:`_frozen_query`).
        flatten: Turns the page into output rows.
        base: Host override (post search lives on a different one).
        opt_in_ttl: Seconds to cache for when the origin declares nothing.
            Ignored when it does declare — the origin's own policy always wins
            over a caller's guess.
        first_page_only: Stop after one page even when a cursor comes back —
            for an endpoint that refuses its own cursor to anonymous callers.
    """
    if state.done:
        out.finish()
        return
    frozen = _frozen_query(state, query)
    hint = CacheHint()
    payload, rows, cursor = api.page(
        method,
        key,
        frozen,
        cursor=state.cursor or None,
        base=base,
        hint=hint,
        page_limit=PAGE_LIMIT,
    )
    state.cursor = cursor or ""
    state.done = cursor is None or first_page_only
    if hint.cacheable:
        cache_control = CacheControl(ttl=hint.max_age, stale_if_error=STALE_IF_ERROR)
    elif opt_in_ttl > 0:
        cache_control = CacheControl(ttl=opt_in_ttl, stale_if_error=STALE_IF_ERROR)
    else:
        cache_control = None
    batch, _ = build_filtered(params, flatten(payload, rows))
    out.emit(batch, cache_control=cache_control)
