"""SQL executed against a real ATTACH.

In vgi-kalshi, which this worker is modelled on, every serious defect was found
by running queries through DuckDB rather than by a unit test — most memorably a
filter-pushdown bug that made an impossible predicate return rows. This file is
the tier that exercises the contract the extension actually holds the worker to.

Marked `live` because a real ATTACH necessarily talks to Bluesky. Runs on
Haybarn, the DuckDB distribution the `vgi` extension is published for; stock
`duckdb` wheels have no build that speaks the current protocol. Set
``VGI_EXTENSION`` to a local build's path to load that instead of installing.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.live

ROOT = Path(__file__).resolve().parent.parent

#: A subquery that always yields a real post.
LATEST = (
    "(SELECT uri FROM bluesky.main.author_feed('bsky.app', filter => 'posts_no_replies') "
    "WHERE reason IS NULL LIMIT 1)"
)


@pytest.fixture(scope="module")
def con() -> Iterator[Any]:
    """A DuckDB connection with the worker attached."""
    haybarn = pytest.importorskip("haybarn")
    connection = haybarn.connect(config={"allow_unsigned_extensions": "true"})
    try:
        if extension := os.environ.get("VGI_EXTENSION"):
            connection.execute(f"LOAD '{extension}'")
        else:
            connection.execute("INSTALL vgi FROM community")
            connection.execute("LOAD vgi")
        connection.execute(
            f"ATTACH 'bluesky' (TYPE vgi, LOCATION 'uv run --project {ROOT} {ROOT / 'bluesky_worker.py'}')"
        )
    except Exception as exc:  # pragma: no cover - environment, not the worker
        pytest.skip(f"cannot attach the worker: {exc}")
    yield connection
    connection.close()


class TestFiltersAreApplied:
    """Declaring `filter_pushdown` makes the engine drop its own filter.

    So a predicate the worker fails to apply is applied by no one.
    """

    def test_an_impossible_predicate_returns_nothing(self, con: Any) -> None:
        got = con.execute(
            "SELECT count(*) FROM bluesky.main.search_posts('bluesky') WHERE like_count < 0"
        ).fetchall()
        assert got == [(0,)]

    def test_an_author_predicate_holds_on_every_row(self, con: Any) -> None:
        got = con.execute(
            "SELECT bool_and(author_handle = 'bsky.app') IS NOT FALSE "
            "FROM bluesky.main.search_posts('bluesky') WHERE author_handle = 'bsky.app'"
        ).fetchall()
        assert got == [(True,)]

    def test_a_time_window_holds_on_every_row(self, con: Any) -> None:
        got = con.execute(
            "SELECT bool_and(created_at < now() - INTERVAL 1 DAY) IS NOT FALSE "
            "FROM bluesky.main.search_posts('bluesky') WHERE created_at < now() - INTERVAL 1 DAY"
        ).fetchall()
        assert got == [(True,)]


class TestScans:
    def test_trends_table(self, con: Any) -> None:
        assert con.execute("SELECT min(rank) FROM bluesky.main.trends").fetchall() == [(1,)]

    def test_a_limit_stops_a_huge_scan(self, con: Any) -> None:
        """bsky.app has tens of millions of followers; this must return promptly."""
        got = con.execute(
            "SELECT count(*) FROM (SELECT did FROM bluesky.main.followers('bsky.app') LIMIT 5)"
        ).fetchall()
        assert got == [(5,)]

    def test_search_returns_at_most_one_page(self, con: Any) -> None:
        (count,) = con.execute("SELECT count(*) FROM bluesky.main.search_posts('bluesky')").fetchone()
        assert 0 < count <= 100


class TestLateral:
    def test_profile_under_lateral(self, con: Any) -> None:
        rows = con.execute(
            "SELECT a.did = p.did FROM (SELECT did FROM bluesky.main.search_actors('bluesky') LIMIT 5) a, "
            "LATERAL bluesky.main.profile(a.did) p"
        ).fetchall()
        assert rows and all(ok for (ok,) in rows)

    def test_thread_anchor(self, con: Any) -> None:
        got = con.execute(f"SELECT count(*) FROM bluesky.main.thread({LATEST}, depth => 0) WHERE depth = 0")
        assert got.fetchall() == [(1,)]


class TestTypes:
    def test_column_types(self, con: Any) -> None:
        got = con.execute(
            "SELECT typeof(created_at), typeof(like_count), typeof(langs) "
            "FROM bluesky.main.search_posts('bluesky') LIMIT 1"
        ).fetchall()
        assert got == [("TIMESTAMP WITH TIME ZONE", "BIGINT", "VARCHAR[]")]


class TestJetstream:
    def test_the_budget_bounds_the_scan(self, con: Any) -> None:
        import time

        started = time.time()
        (count,) = con.execute("SELECT count(*) FROM bluesky.main.jetstream(seconds => 3)").fetchone()
        assert count > 0
        assert time.time() - started < 6, "the scan overran its three-second budget"

    def test_a_limit_ends_a_long_listen_early(self, con: Any) -> None:
        import time

        started = time.time()
        got = con.execute(
            "SELECT count(*) FROM (SELECT * FROM bluesky.main.jetstream(seconds => 60) LIMIT 5)"
        )
        assert got.fetchall() == [(5,)]
        assert time.time() - started < 10

    def test_a_pushed_collection_holds_on_every_commit(self, con: Any) -> None:
        got = con.execute(
            "SELECT count(*) > 0, bool_and(collection = 'app.bsky.feed.like') FROM "
            "bluesky.main.jetstream(seconds => 3) WHERE collection = 'app.bsky.feed.like'"
        ).fetchall()
        assert got == [(True, True)]

    def test_a_replayed_window_stays_inside_its_bounds(self, con: Any) -> None:
        got = con.execute(
            "SELECT count(*) > 0, bool_and(event_time >= now() - INTERVAL 3 MINUTE "
            "AND event_time < now() - INTERVAL 2 MINUTE) FROM bluesky.main.jetstream("
            "collections => 'app.bsky.feed.post', seconds => 60) "
            "WHERE event_time >= now() - INTERVAL 3 MINUTE AND event_time < now() - INTERVAL 2 MINUTE"
        ).fetchall()
        assert got == [(True, True)]

    def test_chained_polls_equal_one_continuous_read(self, con: Any) -> None:
        """Feeding back max(time_us) as `cursor` must neither skip nor repeat an event.

        A fixed window in the recent past is replayed twice — once in one read,
        once as three chained polls — and the two must be the same events.
        """
        import time

        start = int((time.time() - 900) * 1_000_000)
        query = "SELECT time_us FROM bluesky.main.jetstream(cursor => {}, max_events => {}, seconds => 60)"
        whole = sorted(r[0] for r in con.execute(query.format(start, 1500)).fetchall())
        polled: list[int] = []
        cursor = start
        for _ in range(3):
            page = sorted(r[0] for r in con.execute(query.format(cursor, 500)).fetchall())
            polled += page
            cursor = page[-1]
        assert len(whole) == 1500
        assert polled == whole

    def test_an_endless_scan_streams_until_the_caller_stops(self, con: Any) -> None:
        """`seconds => 0` never ends on its own; rows must still reach the caller as they arrive.

        DuckDB's streaming result keeps executing until `streaming_buffer_size`
        (~1 MB) has accumulated, which a firehose of small rows takes a long
        time to fill — with the default, nothing reaches the client for many
        seconds. A small buffer is what a live consumer sets.
        """
        import time

        con.execute("SET streaming_buffer_size = '1KB'")
        try:
            started = time.time()
            reader = con.execute(
                "SELECT time_us FROM bluesky.main.jetstream(seconds => 0, batch_ms => 500)"
            ).to_arrow_reader(100)
            arrivals: list[float] = []
            for batch in reader:
                if batch.num_rows:
                    arrivals.append(time.time() - started)
                if time.time() - started > 5:
                    break
            reader.close()
        finally:
            con.execute("RESET streaming_buffer_size")
        assert arrivals and arrivals[0] < 4, f"first rows only after {arrivals[:1]}s"
        assert len({round(a) for a in arrivals}) >= 2, "rows arrived in one lump, not as a stream"
        # The connection is still usable after walking away from an endless scan.
        assert con.execute("SELECT count(*) > 0 FROM bluesky.main.trends").fetchall() == [(True,)]

    def test_a_filtered_endless_listen_streams_too(self, con: Any) -> None:
        """A topic listen: a selective filter over an endless scan must still stream.

        FILTER is one of DuckDB's caching operators — it holds chunks of 64 rows or
        fewer until it has a full vector — so without `enable_caching_operators =
        false` a keyword match delivers nothing until the scan ends, which for an
        endless scan is never.
        """
        import time

        con.execute("SET streaming_buffer_size = '1KB'")
        con.execute("SET enable_caching_operators = false")
        try:
            started = time.time()
            reader = con.execute(
                "SELECT text FROM bluesky.main.jetstream(collections => 'app.bsky.feed.post', "
                "seconds => 0, batch_ms => 500) WHERE text ILIKE '%the%'"
            ).to_arrow_reader(10)
            arrivals: list[float] = []
            for batch in reader:
                if batch.num_rows:
                    arrivals.append(time.time() - started)
                if time.time() - started > 12:
                    break
            reader.close()
        finally:
            con.execute("RESET streaming_buffer_size")
            con.execute("RESET enable_caching_operators")
        # Measured: a filtered listen starts ~4 s later than an unfiltered one
        # (first rows at ~6 s against ~2 s), then arrives every 0.5-1 s.
        assert arrivals and arrivals[0] < 10, f"first filtered rows only after {arrivals[:1]}s"
        assert len({round(a) for a in arrivals}) >= 3, "filtered rows arrived in one lump"
