"""Accounts: profile lookup, actor search, and the follow graph.

``profile()`` is **blended** (a :class:`~vgi.table_in_out_function.RowTransformFunction`):
its positional argument *is* the per-row input column, so one registration
serves both a literal call and a correlated LATERAL, and a whole input batch is
answered by ``getProfiles`` 25 actors at a time::

    SELECT * FROM bluesky.main.profile('bsky.app');

    SELECT p.handle, p.followers_count
    FROM bluesky.main.search_actors('duckdb') a,
         LATERAL bluesky.main.profile(a.did) p;

``search_actors``, ``followers`` and ``follows`` are cursor-paged endpoints and
so are stateful scans — see :mod:`vgi_bluesky.paging` for why.

Every actor argument accepts a handle, a DID, ``@handle``, or a
``https://bsky.app/profile/...`` URL.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import BindParams, ProcessParams, TableFunctionGenerator, init_single_worker
from vgi.table_in_out_function import RowTransformFunction
from vgi_rpc.rpc import OutputCollector

from vgi_bluesky import bluesky_api as api
from vgi_bluesky.bluesky_api import CacheHint
from vgi_bluesky.meta import docs, examples
from vgi_bluesky.output import blended_cache_control, emit_fanout
from vgi_bluesky.paging import PagedScanState, emit_page
from vgi_bluesky.schemas import (
    ACTOR_SCHEMA,
    GRAPH_SCHEMA,
    PROFILE_SCHEMA,
    flatten_actor,
    flatten_graph,
    flatten_profile,
)


@dataclass(slots=True, frozen=True, kw_only=True)
class ActorArgs:
    """A lone actor input column."""

    actor: Annotated[str, Arg(0, doc="Handle, DID, @handle or bsky.app profile URL")]


class ProfileFunction(RowTransformFunction[ActorArgs]):
    """Detailed profiles, batched 25 to a request — 1->0/1 per input actor."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = PROFILE_SCHEMA

    class Meta:
        name = "profile"
        description = "One Bluesky account's profile and follower counts, by handle or DID"
        categories = ["actors", "blended"]
        projection_pushdown = True
        tags = docs(
            category="actors",
            result_schema=PROFILE_SCHEMA,
            llm=(
                "The full profile of one account — bio, follower/following/post counts, and "
                "verification — by handle or DID. Reach for this to size up an account or to turn a "
                "handle into the permanent `did`. It composes under a correlated LATERAL, so it "
                "enriches the actors any other function returns with their counts."
            ),
            md=(
                "One row per account that exists; an unknown or deleted account returns no row "
                "rather than an error.\n\n"
                "### Handles versus DIDs\n\n"
                "A handle (`bsky.app`) is a DNS name the account can change at any time; the `did` "
                "is permanent. Store and join on `did`. The `actor` column echoes the input exactly, "
                "so a LATERAL can still be matched back to what was passed in.\n\n"
                "### Cost under LATERAL\n\n"
                "Profiles are fetched 25 per request, so a LATERAL over 100 actors is four calls, "
                "not a hundred. Results are cached for the 30 seconds the AppView declares."
            ),
            example_queries=examples(
                (
                    "The official Bluesky account's counts",
                    "SELECT handle, display_name, followers_count, follows_count, posts_count "
                    "FROM bluesky.main.profile('bsky.app')",
                ),
                (
                    "Follower counts for every account a search finds, via LATERAL",
                    "SELECT p.handle, p.followers_count FROM ("
                    "SELECT did FROM bluesky.main.search_actors('duckdb') LIMIT 20) a, "
                    "LATERAL bluesky.main.profile(a.did) p ORDER BY p.followers_count DESC",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT handle, display_name, followers_count, follows_count, posts_count "
                    "FROM bluesky.main.profile('bsky.app')"
                ),
                description="The official Bluesky account's counts",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ActorArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[ActorArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        inputs = batch.column("actor").to_pylist()
        wanted = {index: api.normalize_actor(str(value)) for index, value in enumerate(inputs) if value}
        hint = CacheHint()
        with api.open_client() as client:
            found = api.profiles([a for a in wanted.values() if a], client=client, hint=hint)
        # getProfiles returns only the accounts it found, in no promised order,
        # so rows are matched back by DID or by (case-insensitive) handle.
        by_key: dict[str, dict[str, Any]] = {}
        for view in found:
            for key in (view.get("did"), str(view.get("handle") or "").lower()):
                if key:
                    by_key[key] = view
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        for index, actor in wanted.items():
            view = by_key.get(actor)
            if view is not None:
                rows.append(flatten_profile(str(inputs[index]), view))
                parents.append(index)
        emit_fanout(out, params, rows, parents, blended_cache_control(hint))


@dataclass(slots=True, frozen=True, kw_only=True)
class SearchActorsArgs:
    """``search_actors(query)``."""

    query: Annotated[str, Arg(0, doc="Search terms matched against handles, names and bios")]


@init_single_worker
class SearchActorsFunction(TableFunctionGenerator[SearchActorsArgs, PagedScanState]):
    """Accounts matching a search, one API page per tick."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = ACTOR_SCHEMA

    class Meta:
        name = "search_actors"
        description = "Search Bluesky accounts by handle, display name and bio"
        categories = ["actors"]
        projection_pushdown = True
        tags = docs(
            category="actors",
            result_schema=ACTOR_SCHEMA,
            llm=(
                "Find accounts by name or topic. Matches handles, display names and bios, and "
                "returns them in Bluesky's relevance order. This is how to get from a name to a "
                "`did` or `handle` when you do not already know it; feed that into `profile()` for "
                "counts or `author_feed()` for posts."
            ),
            md=(
                "Account search, in Bluesky's own relevance order.\n\n"
                "### Streaming\n\n"
                "Rows arrive one API page (100 accounts) at a time, so a `LIMIT` stops early. The "
                "cost is that this cannot be the inner side of a correlated `LATERAL` — drive a "
                "join from it, not into it.\n\n"
                "### Counts are not here\n\n"
                "Search returns the basic actor view, which carries no follower counts. Join to "
                "`profile()` under a LATERAL to add them."
            ),
            example_queries=examples(
                (
                    "Accounts that talk about DuckDB",
                    "SELECT handle, display_name, description "
                    "FROM bluesky.main.search_actors('duckdb') LIMIT 20",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT handle, display_name, description "
                    "FROM bluesky.main.search_actors('duckdb') LIMIT 20"
                ),
                description="Accounts that talk about DuckDB",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[SearchActorsArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[SearchActorsArgs]) -> PagedScanState:
        return PagedScanState()

    @classmethod
    def process(
        cls, params: ProcessParams[SearchActorsArgs], state: PagedScanState, out: OutputCollector
    ) -> None:
        """Emit one page of matching accounts."""
        emit_page(
            params,
            state,
            out,
            method="app.bsky.actor.searchActors",
            key="actors",
            query={"q": params.args.query},
            flatten=lambda _payload, rows: [flatten_actor(row) for row in rows],
        )


def _graph_flatten(payload: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Follower/follow rows, each stamped with the account whose graph they came from.

    The listed accounts do not say whose list they are on; the payload's
    ``subject`` does. Stamping it on makes the rows self-describing, and gives a
    caller who passed a handle the subject's permanent DID.
    """
    subject = payload.get("subject") if isinstance(payload.get("subject"), dict) else {}
    return [flatten_graph(subject, row) for row in rows]


_GRAPH_MD = (
    "### Streaming, and why that matters here\n\n"
    "A large account has millions of followers. Rows arrive one API page (100 accounts) at a "
    "time, so a `LIMIT` stops the walk early — but an aggregate such as `count(*)` over a large "
    "account will walk every page. Use `profile()`'s `followers_count` / `follows_count` for "
    "counts.\n\n"
    "Because it streams, this cannot be the inner side of a correlated `LATERAL`."
)


@init_single_worker
class FollowersFunction(TableFunctionGenerator[ActorArgs, PagedScanState]):
    """Accounts following an actor, newest first, one API page per tick."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = GRAPH_SCHEMA

    class Meta:
        name = "followers"
        description = "Accounts that follow a Bluesky account, most recent first"
        categories = ["graph"]
        projection_pushdown = True
        tags = docs(
            category="graph",
            result_schema=GRAPH_SCHEMA,
            llm=(
                "Who follows an account, most recent follows first. Reach for this to see an "
                "account's audience or recent follower growth. For just the number, use "
                "`profile().followers_count` — this lists them one by one and a large account has "
                "millions."
            ),
            md="Accounts following the given actor, most recent first.\n\n" + _GRAPH_MD,
            example_queries=examples(
                (
                    "The 50 most recent followers of an account",
                    "SELECT handle, display_name FROM bluesky.main.followers('bsky.app') LIMIT 50",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql="SELECT handle, display_name FROM bluesky.main.followers('bsky.app') LIMIT 50",
                description="The 50 most recent followers of an account",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ActorArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[ActorArgs]) -> PagedScanState:
        return PagedScanState()

    @classmethod
    def process(cls, params: ProcessParams[ActorArgs], state: PagedScanState, out: OutputCollector) -> None:
        """Emit one page of followers."""
        emit_page(
            params,
            state,
            out,
            method="app.bsky.graph.getFollowers",
            key="followers",
            query={"actor": api.normalize_actor(params.args.actor)},
            flatten=_graph_flatten,
        )


@init_single_worker
class FollowsFunction(TableFunctionGenerator[ActorArgs, PagedScanState]):
    """Accounts an actor follows, newest first, one API page per tick."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = GRAPH_SCHEMA

    class Meta:
        name = "follows"
        description = "Accounts a Bluesky account follows, most recent first"
        categories = ["graph"]
        projection_pushdown = True
        tags = docs(
            category="graph",
            result_schema=GRAPH_SCHEMA,
            llm=(
                "Who an account follows, most recently followed first. Reach for this to see what "
                "an account reads or to find related accounts. For just the number, use "
                "`profile().follows_count`."
            ),
            md="Accounts the given actor follows, most recent first.\n\n" + _GRAPH_MD,
            example_queries=examples(
                (
                    "Accounts the official Bluesky account follows",
                    "SELECT handle, display_name FROM bluesky.main.follows('bsky.app') LIMIT 100",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql="SELECT handle, display_name FROM bluesky.main.follows('bsky.app') LIMIT 100",
                description="Accounts the official Bluesky account follows",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ActorArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[ActorArgs]) -> PagedScanState:
        return PagedScanState()

    @classmethod
    def process(cls, params: ProcessParams[ActorArgs], state: PagedScanState, out: OutputCollector) -> None:
        """Emit one page of followed accounts."""
        emit_page(
            params,
            state,
            out,
            method="app.bsky.graph.getFollows",
            key="follows",
            query={"actor": api.normalize_actor(params.args.actor)},
            flatten=_graph_flatten,
        )


ACTOR_FUNCTIONS: list[type] = [ProfileFunction, SearchActorsFunction, FollowersFunction, FollowsFunction]
