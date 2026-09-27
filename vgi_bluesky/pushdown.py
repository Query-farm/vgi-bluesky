"""Turning a DuckDB ``WHERE`` into XRPC query parameters, and applying it to rows.

Pushdown here is always a pure optimisation, which makes the two directions of
error wildly asymmetric:

* Pushing **too little** costs bandwidth and rate-limit budget. Nothing else.
* Pushing **too much** drops rows the predicate would have kept, and nothing
  downstream can recover what was never fetched.

So every helper here declines when it is not certain. A filter is translated
only when the API parameter means exactly what the SQL predicate means, or
strictly more.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from vgi.table_function import ProcessParams

from vgi_bluesky.schemas import batch_from_rows


def _filters(params: ProcessParams[Any]) -> Any:
    return getattr(params, "current_pushdown_filters", None)


def equality(params: ProcessParams[Any], column: str) -> str | None:
    """The constant from ``WHERE <column> = '...'``, when there is exactly one.

    Only string equality is returned; anything else is left for DuckDB. An
    empty string is treated as absent, matching the sentinel convention the
    argument dataclasses use for "not supplied".
    """
    filters = _filters(params)
    if filters is None:
        return None
    scalar = filters.get_column_constant(column)
    value = scalar.as_py() if scalar is not None else None
    return str(value) if isinstance(value, str) and value else None


def iso(moment: datetime) -> str:
    """An RFC 3339 UTC timestamp, as XRPC ``datetime`` parameters want it."""
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


def datetime_bounds(params: ProcessParams[Any], column: str) -> tuple[datetime | None, datetime | None]:
    """``(since, until)`` from range predicates on a timestamp column.

    Returned as datetimes so a caller can combine bounds from several columns
    by comparing instants; format with :func:`iso` at the end. (Comparing the
    formatted strings is wrong: ``...:00Z`` sorts after ``...:00.5Z``.)

    ``searchPosts`` treats ``since`` as inclusive and ``until`` as exclusive.
    A pushed upper bound may be inclusive (``<=``), so ``until`` is widened by a
    second: fetching one extra second of posts is free, and DuckDB's own filter
    removes it. Narrowing instead would drop a post sitting on the boundary.

    Returns ``(None, None)`` when nothing usable was pushed.
    """
    filters = _filters(params)
    if filters is None:
        return None, None
    bounds = filters.get_column_bounds(column)
    if bounds is None:
        return None, None

    def moment(scalar: Any) -> datetime | None:
        value = scalar.as_py() if scalar is not None else None
        return value if isinstance(value, datetime) else None

    low = moment(getattr(bounds, "min_value", None))
    high = moment(getattr(bounds, "max_value", None))
    return low, (high + timedelta(seconds=1) if high is not None else None)


def build_filtered(
    params: ProcessParams[Any],
    rows: Sequence[dict[str, Any]],
    parent_rows: Sequence[int] | None = None,
) -> tuple[pa.RecordBatch, list[int]]:
    """Build the projected output batch, applying every pushed filter to it.

    **Declaring ``filter_pushdown`` is a promise to apply the filters.** The
    engine drops its own filter above the scan once a function accepts
    pushdown, so a predicate the worker receives and ignores is not re-checked
    by anyone — it simply stops being applied. Translating *some* predicates
    into query parameters and leaving the rest would be a wrong answer, not a
    partial optimisation.

    Everything is evaluated against ``params.output_schema`` — the *projected*
    schema. A pushed filter carries a ``column_index`` relative to the
    projection DuckDB asked for, so evaluating it against any other column
    order would compare the wrong column. DuckDB never projects away a column
    it filters on, so the projected batch always holds what the predicate needs.

    Args:
        params: The tick's parameters, carrying the filters and the projection.
        rows: Flattened rows, keyed by the schema's column names.
        parent_rows: 1->N provenance, filtered in lockstep with the rows so a
            blended function's mapping survives.

    Returns:
        The projected, filtered batch and its surviving ``parent_rows``.
    """
    batch = batch_from_rows(rows, params.output_schema)
    filters = _filters(params)
    if filters is None:
        return batch, list(parent_rows or [])
    mask = filters.evaluate(batch)
    projected = pc.filter(batch, mask)
    if parent_rows is None:
        return projected, []
    # `pc.filter` drops rows whose mask is null, which is the SQL meaning of a
    # predicate that did not evaluate to true; provenance follows the same rule.
    keep = mask.to_pylist()
    return projected, [parent for parent, ok in zip(parent_rows, keep, strict=True) if ok]
