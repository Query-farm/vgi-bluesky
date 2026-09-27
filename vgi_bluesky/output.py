"""Emitting results from blended (row-transform) functions.

A blended function is 1->N: one input row fans out to any number of output rows
(a thread is many posts; an unknown profile is none), so every ``emit`` carries
``parent_rows`` provenance mapping each output row back to the input row that
produced it. Without it the batched-LATERAL operator cannot stamp the
correlated columns onto the right rows.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast

from vgi.cache_control import CacheControl
from vgi.table_function import ProcessParams
from vgi_rpc.rpc import OutputCollector

from vgi_bluesky.bluesky_api import STALE_IF_ERROR, CacheHint
from vgi_bluesky.pushdown import build_filtered

if TYPE_CHECKING:
    from vgi.protocol import VgiOutputCollector


def blended_cache_control(hint: CacheHint, opt_in_ttl: int = 0) -> CacheControl | None:
    """Cache metadata for a blended lookup: the origin's policy, else the caller's opt-in.

    The public AppView declares ``max-age=30`` on every query, and that is
    forwarded rather than invented. ``per_value`` memoization makes a repeat of
    the same input under a LATERAL a cache hit instead of another request.
    """
    if hint.cacheable:
        return CacheControl(ttl=hint.max_age, stale_if_error=STALE_IF_ERROR, per_value=True)
    if opt_in_ttl > 0:
        return CacheControl(ttl=opt_in_ttl, stale_if_error=STALE_IF_ERROR, per_value=True)
    return None


def emit_fanout(
    out: OutputCollector,
    params: ProcessParams[Any],
    rows: Sequence[dict[str, Any]],
    parent_rows: Sequence[int],
    cache_control: CacheControl | None = None,
) -> None:
    """Emit a 1->N batch with per-output-row provenance and optional cacheability.

    ``parent_rows[i]`` is the index, within this call's input batch, of the row
    that produced output row ``i``. Any pushed filter is applied here too, and
    ``parent_rows`` is filtered in lockstep.
    """
    batch, parents = build_filtered(params, rows, parent_rows)
    cast("VgiOutputCollector", out).emit(batch, parent_rows=parents, cache_control=cache_control)
