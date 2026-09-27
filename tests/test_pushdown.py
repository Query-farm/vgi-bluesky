"""Filter pushdown: which WHERE clauses become searchPosts parameters, and that all are applied.

Pushing too little costs bandwidth; pushing too much drops rows that nothing can
recover. ``searchPosts`` filters on ``sortAt`` — the earlier of a post's
``createdAt`` and ``indexedAt`` — so most of these cases are about declining.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from vgi_bluesky.posts import SearchPostsArgs, _search_window
from vgi_bluesky.pushdown import build_filtered, datetime_bounds, equality
from vgi_bluesky.schemas import POST_SCHEMA


@dataclass
class _Bounds:
    min_value: pa.Scalar[Any] | None = None
    max_value: pa.Scalar[Any] | None = None


class _Filters:
    """The slice of PushdownFilters this code uses."""

    def __init__(
        self,
        constants: dict[str, str] | None = None,
        bounds: dict[str, tuple[datetime | None, datetime | None]] | None = None,
        keep: str | None = None,
    ) -> None:
        self._constants = constants or {}
        self._bounds = bounds or {}
        self._keep = keep

    def get_column_constant(self, column: str) -> pa.Scalar[Any] | None:
        value = self._constants.get(column)
        return pa.scalar(value) if value is not None else None

    def get_column_bounds(self, column: str) -> _Bounds | None:
        if column not in self._bounds:
            return None
        low, high = self._bounds[column]
        ts = pa.timestamp("us", tz="UTC")
        return _Bounds(
            pa.scalar(low, type=ts) if low else None,
            pa.scalar(high, type=ts) if high else None,
        )

    def evaluate(self, batch: pa.RecordBatch) -> pa.BooleanArray:
        if self._keep is None:
            return pa.array([True] * batch.num_rows)
        return pc.equal(batch.column("author_handle"), self._keep)


@dataclass
class _Params:
    args: Any = None
    current_pushdown_filters: Any = None
    output_schema: pa.Schema = POST_SCHEMA


SEP1 = datetime(2026, 9, 1, tzinfo=UTC)
SEP2 = datetime(2026, 9, 2, tzinfo=UTC)
SEP20 = datetime(2026, 9, 20, tzinfo=UTC)


def _window(filters: _Filters | None, **args: str) -> tuple[str | None, str | None]:
    return _search_window(_Params(args=SearchPostsArgs(query="x", **args), current_pushdown_filters=filters))  # type: ignore[arg-type]


class TestSearchWindow:
    def test_nothing_pushed(self) -> None:
        assert _window(None) == (None, None)

    def test_an_upper_bound_on_created_at_becomes_until(self) -> None:
        """sortAt <= createdAt <= bound, so until is safe — widened a second for an inclusive <=."""
        assert _window(_Filters(bounds={"created_at": (None, SEP20)})) == (None, "2026-09-20T00:00:01Z")

    def test_a_lower_bound_on_created_at_alone_is_not_pushed(self) -> None:
        """A future-dated createdAt has a sortAt below it; since would drop that post."""
        assert _window(_Filters(bounds={"created_at": (SEP1, None)})) == (None, None)

    def test_lower_bounds_on_both_columns_push_the_smaller(self) -> None:
        filters = _Filters(bounds={"created_at": (SEP2, None), "indexed_at": (SEP1, None)})
        assert _window(filters) == ("2026-09-01T00:00:00Z", None)

    def test_bounds_are_compared_as_instants_not_strings(self) -> None:
        """As strings, '...00Z' sorts after '...00.500000Z' — the wrong answer."""
        half = datetime(2026, 9, 1, 0, 0, 0, 500000, tzinfo=UTC)
        filters = _Filters(bounds={"created_at": (SEP1, None), "indexed_at": (half, None)})
        assert _window(filters)[0] == "2026-09-01T00:00:00Z"

    def test_the_tighter_upper_bound_wins(self) -> None:
        filters = _Filters(bounds={"created_at": (None, SEP20), "indexed_at": (None, SEP2)})
        assert _window(filters)[1] == "2026-09-02T00:00:01Z"

    def test_explicit_arguments_win(self) -> None:
        filters = _Filters(bounds={"created_at": (SEP1, SEP20), "indexed_at": (SEP1, SEP20)})
        assert _window(filters, since="2025-01-01", until="2025-02-01") == ("2025-01-01", "2025-02-01")


class TestHelpers:
    def test_equality_ignores_non_strings_and_empties(self) -> None:
        params = _Params(current_pushdown_filters=_Filters(constants={"author_handle": "a.test", "lang": ""}))
        assert equality(params, "author_handle") == "a.test"  # type: ignore[arg-type]
        assert equality(params, "lang") is None  # type: ignore[arg-type]
        assert equality(params, "missing") is None  # type: ignore[arg-type]

    def test_datetime_bounds_widen_the_upper_bound(self) -> None:
        params = _Params(current_pushdown_filters=_Filters(bounds={"created_at": (SEP1, SEP2)}))
        low, high = datetime_bounds(params, "created_at")  # type: ignore[arg-type]
        assert low == SEP1 and high is not None and (high - SEP2).total_seconds() == 1

    def test_datetime_bounds_without_filters(self) -> None:
        assert datetime_bounds(_Params(), "created_at") == (None, None)  # type: ignore[arg-type]


class TestFiltersAreApplied:
    """Declaring filter_pushdown makes the engine drop its own filter above the scan.

    So every pushed predicate must be applied to the rows here, not merely
    translated where it can be — and 1->N provenance must follow the rows.
    """

    ROWS = [{"author_handle": "a.test"}, {"author_handle": "b.test"}, {"author_handle": "a.test"}]

    def test_rows_are_filtered(self) -> None:
        params = _Params(current_pushdown_filters=_Filters(keep="a.test"))
        batch, _ = build_filtered(params, self.ROWS)  # type: ignore[arg-type]
        assert batch.column("author_handle").to_pylist() == ["a.test", "a.test"]

    def test_provenance_follows_the_rows(self) -> None:
        params = _Params(current_pushdown_filters=_Filters(keep="a.test"))
        _, parents = build_filtered(params, self.ROWS, [7, 8, 9])  # type: ignore[arg-type]
        assert parents == [7, 9]

    def test_the_projection_is_honoured(self) -> None:
        projected = pa.schema([POST_SCHEMA.field("author_handle")])
        batch, _ = build_filtered(_Params(output_schema=projected), self.ROWS)  # type: ignore[arg-type]
        assert batch.schema.names == ["author_handle"]
