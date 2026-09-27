"""Custom feeds and trending topics.

Bluesky's custom feeds are algorithms anyone can publish: a *feed generator* is
a record declaring the feed, pointing at a service that computes it. The
AppView proxies ``getFeed`` to that service and hydrates the post references it
returns, so a feed's contents are only as available as the service behind it.

``trends`` is the one unkeyed, small, fast-changing surface here, so it is a
real catalog table backed by ``all_trends``, the way ``series`` is in
``vgi-kalshi``. ``ReadOnlyCatalogInterface.table_scan_function_get`` auto-wires
the scan; the backing function takes the ``all_`` prefix because a function
and a table cannot share one name in a schema.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, ClassVar

import pyarrow as pa
from vgi.arguments import Arg
from vgi.cache_control import CacheControl
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import BindParams, ProcessParams, TableFunctionGenerator, init_single_worker
from vgi_rpc.rpc import OutputCollector

from vgi_bluesky import bluesky_api as api
from vgi_bluesky.bluesky_api import STALE_IF_ERROR, CacheHint
from vgi_bluesky.meta import docs, examples
from vgi_bluesky.paging import PagedScanState, emit_page
from vgi_bluesky.posts import feed_rows
from vgi_bluesky.pushdown import build_filtered
from vgi_bluesky.schemas import (
    FEED_GENERATOR_SCHEMA,
    FEED_ITEM_SCHEMA,
    TREND_SCHEMA,
    flatten_feed_generator,
    flatten_trends,
)

#: The feed the app calls "Discover" — the stable example for docs and tests.
DISCOVER_FEED = "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.generator/whats-hot"


def _require_feed_uri(value: str) -> str:
    """A feed reference as a DID-authority AT-URI, or a clear error naming the input."""
    parts = api.post_uri_parts(value)
    if parts is None or parts[1] != "app.bsky.feed.generator":
        raise ValueError(
            f"{value!r} is not a feed reference; pass at://<did>/app.bsky.feed.generator/<rkey> "
            "or https://bsky.app/profile/<handle>/feed/<rkey>"
        )
    uri = api.canonical_uri(value, dids={})
    if uri is None:
        raise ValueError(f"the handle in {value!r} does not resolve to an account")
    return uri


@dataclass(slots=True, frozen=True, kw_only=True)
class FeedArgs:
    """``feed(uri)``."""

    uri: Annotated[str, Arg(0, doc="Feed AT-URI or bsky.app feed URL")]


@init_single_worker
class FeedFunction(TableFunctionGenerator[FeedArgs, PagedScanState]):
    """The posts a custom feed currently serves, in the feed's own order."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = FEED_ITEM_SCHEMA

    class Meta:
        name = "feed"
        description = "The posts a Bluesky custom feed is currently serving, in feed order"
        categories = ["feeds"]
        projection_pushdown = True
        tags = docs(
            category="feeds",
            result_schema=FEED_ITEM_SCHEMA,
            llm=(
                "Read a custom feed — such as Discover, or any community feed — as its algorithm "
                "serves it to a logged-out visitor, in the feed's own ranking order. Reach for "
                "this to see what a feed is surfacing right now. Find feed URIs with "
                "`popular_feeds()` or `actor_feeds()`."
            ),
            md=(
                "The posts a feed generator returns, hydrated into full post rows.\n\n"
                "### Order is the feed's\n\n"
                "Rows come back in the order the feed's algorithm ranked them, not by time. Keep "
                "that order with `row_number() OVER ()` before you sort by anything else.\n\n"
                "### The feed must be a literal\n\n"
                "Pass the feed as a literal AT-URI or bsky.app feed URL: DuckDB does not accept a "
                "subquery as this function's argument. Look the URI up first, then call this with "
                "it.\n\n"
                "### Availability\n\n"
                "A feed is computed by its publisher's own service; the AppView only forwards "
                "the request. A feed whose service is down fails, and feeds personalised to a "
                "logged-in viewer show their logged-out fallback."
            ),
            example_queries=examples(
                (
                    "What the Discover feed is showing right now",
                    f"SELECT author_handle, text, like_count FROM bluesky.main.feed('{DISCOVER_FEED}') "
                    "LIMIT 30",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT author_handle, text, like_count FROM bluesky.main.feed('{DISCOVER_FEED}') "
                    "LIMIT 30"
                ),
                description="What the Discover feed is showing right now",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[FeedArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[FeedArgs]) -> PagedScanState:
        return PagedScanState()

    @classmethod
    def process(cls, params: ProcessParams[FeedArgs], state: PagedScanState, out: OutputCollector) -> None:
        """Emit one page of the feed."""
        emit_page(
            params,
            state,
            out,
            method="app.bsky.feed.getFeed",
            key="feed",
            query=lambda: {"feed": _require_feed_uri(params.args.uri)},
            flatten=feed_rows,
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class PopularFeedsArgs:
    """``popular_feeds()`` with an optional search query."""

    query: Annotated[
        str, Arg("query", doc="Search feed names and descriptions (empty = most popular overall)", default="")
    ] = ""


@init_single_worker
class PopularFeedsFunction(TableFunctionGenerator[PopularFeedsArgs, PagedScanState]):
    """Custom feeds, most popular first, optionally matching a search."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = FEED_GENERATOR_SCHEMA

    class Meta:
        name = "popular_feeds"
        description = "Bluesky custom feeds by popularity, optionally searched by name"
        categories = ["feeds"]
        projection_pushdown = True
        tags = docs(
            category="feeds",
            result_schema=FEED_GENERATOR_SCHEMA,
            llm=(
                "Discover custom feeds: the most popular ones overall, or those matching "
                "`query => '...'`. Reach for this to find a feed's `uri` to pass to `feed()`."
            ),
            md=(
                "Feed generators in Bluesky's popularity order, or matching a search when `query` "
                "is given.\n\n"
                "### Streaming\n\n"
                "Rows arrive 100 at a time, so a `LIMIT` stops early."
            ),
            example_queries=examples(
                (
                    "The most popular custom feeds",
                    "SELECT display_name, creator_handle, like_count "
                    "FROM bluesky.main.popular_feeds() LIMIT 25",
                ),
                (
                    "Feeds about science",
                    "SELECT display_name, description, uri "
                    "FROM bluesky.main.popular_feeds(query => 'science') LIMIT 25",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT display_name, creator_handle, like_count "
                    "FROM bluesky.main.popular_feeds() LIMIT 25"
                ),
                description="The most popular custom feeds",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[PopularFeedsArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[PopularFeedsArgs]) -> PagedScanState:
        return PagedScanState()

    @classmethod
    def process(
        cls, params: ProcessParams[PopularFeedsArgs], state: PagedScanState, out: OutputCollector
    ) -> None:
        """Emit one page of feeds."""
        emit_page(
            params,
            state,
            out,
            method="app.bsky.unspecced.getPopularFeedGenerators",
            key="feeds",
            query={"query": params.args.query or None},
            flatten=lambda _payload, rows: [flatten_feed_generator(row) for row in rows],
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class ActorFeedsArgs:
    """``actor_feeds(actor)``."""

    actor: Annotated[str, Arg(0, doc="Handle, DID, @handle or bsky.app profile URL")]


@init_single_worker
class ActorFeedsFunction(TableFunctionGenerator[ActorFeedsArgs, PagedScanState]):
    """Custom feeds an account has published."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = FEED_GENERATOR_SCHEMA

    class Meta:
        name = "actor_feeds"
        description = "Custom feeds a Bluesky account has published"
        categories = ["feeds"]
        projection_pushdown = True
        tags = docs(
            category="feeds",
            result_schema=FEED_GENERATOR_SCHEMA,
            llm=(
                "The custom feeds one account has published. Reach for this when you know who "
                "runs a feed but not its URI; pass the `uri` to `feed()` to read it."
            ),
            md=(
                "Feed generators published by the given account. Most accounts publish none, so "
                "an empty result is normal."
            ),
            example_queries=examples(
                (
                    "Feeds published by the official Bluesky account",
                    "SELECT display_name, description, like_count, uri "
                    "FROM bluesky.main.actor_feeds('bsky.app')",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT display_name, description, like_count, uri "
                    "FROM bluesky.main.actor_feeds('bsky.app')"
                ),
                description="Feeds published by the official Bluesky account",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ActorFeedsArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[ActorFeedsArgs]) -> PagedScanState:
        return PagedScanState()

    @classmethod
    def process(
        cls, params: ProcessParams[ActorFeedsArgs], state: PagedScanState, out: OutputCollector
    ) -> None:
        """Emit one page of the account's feeds."""
        emit_page(
            params,
            state,
            out,
            method="app.bsky.feed.getActorFeeds",
            key="feeds",
            query={"actor": api.normalize_actor(params.args.actor)},
            flatten=lambda _payload, rows: [flatten_feed_generator(row) for row in rows],
            actor=params.args.actor,
        )


@init_single_worker
class AllTrendsFunction(TableFunctionGenerator[None, None]):
    """Bluesky's current trending topics — the scan behind the ``trends`` table."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = TREND_SCHEMA

    class Meta:
        name = "all_trends"
        description = "Bluesky's current trending topics (the scan backing the `trends` table)"
        categories = ["trends"]
        projection_pushdown = True
        tags = docs(
            category="trends",
            result_schema=TREND_SCHEMA,
            llm=(
                "What is trending on Bluesky right now, ranked. Prefer the `trends` table, which "
                "scans this and reads as plain SQL; this form exists for the rare case you want "
                "the scan itself."
            ),
            md=(
                "Bluesky's current trending topics, up to 25, in rank order.\n\n"
                "### Prefer the table\n\n"
                "The `trends` catalog table is backed by this function and returns exactly the "
                "same rows; it is named `all_trends` only so the two can coexist in one schema."
            ),
            example_queries=examples(
                (
                    "Trending topics, read straight from the scan function",
                    "SELECT rank, display_name, post_count FROM bluesky.main.all_trends() ORDER BY rank",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql="SELECT rank, display_name, post_count FROM bluesky.main.all_trends() ORDER BY rank",
                description="Trending topics, read straight from the scan function",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, out: OutputCollector) -> None:
        """Fetch the trend list, forwarding the AppView's own 30-second freshness."""
        hint = CacheHint()
        rows = flatten_trends(api.trends(hint=hint))
        batch, _ = build_filtered(params, rows)
        cache_control = (
            CacheControl(ttl=hint.max_age, stale_if_error=STALE_IF_ERROR) if hint.cacheable else None
        )
        out.emit(batch, cache_control=cache_control)
        out.finish()


FEED_FUNCTIONS: list[type] = [FeedFunction, PopularFeedsFunction, ActorFeedsFunction, AllTrendsFunction]
