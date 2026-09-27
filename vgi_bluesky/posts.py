"""Posts: lookup, threads, author timelines, search, and who interacted with a post.

``post()`` and ``thread()`` are **blended** and compose under a correlated
LATERAL; everything backed by a cursor-paged endpoint is a stateful scan (see
:mod:`vgi_bluesky.paging`).

Every post argument accepts either an AT-URI (``at://<did-or-handle>/app.bsky.feed.post/<rkey>``)
or the web URL the app shows (``https://bsky.app/profile/<handle>/post/<rkey>``).
Handles are resolved to DIDs before the call, because ``getPosts`` answers
HTTP 500 to a handle-authority URI.
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
from vgi_bluesky.pushdown import datetime_bounds, equality, iso
from vgi_bluesky.schemas import (
    FEED_ITEM_SCHEMA,
    INTERACTION_SCHEMA,
    POST_SCHEMA,
    THREAD_SCHEMA,
    flatten_feed_item,
    flatten_like,
    flatten_post,
    flatten_reposter,
    flatten_thread,
)

_POST_REF_DOC = "Post AT-URI or bsky.app post URL"

#: bsky.app's pinned welcome post (October 2024) — a literal post reference
#: stable enough for examples. The listing functions need a literal: DuckDB
#: rejects a subquery as a table function's argument, and only the blended
#: lookups can take a column instead.
WELCOME_POST = "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/3l6oveex3ii2l"


def _require_post_uri(value: str) -> str:
    """A post reference as a DID-authority AT-URI, or a clear error naming the input.

    Used by the paged scans, which take one literal post and have nothing to
    emit for a bad one — unlike the blended lookups, which skip a bad row and
    carry on with the rest of their batch.
    """
    parts = api.post_uri_parts(value)
    if parts is None or parts[1] != "app.bsky.feed.post":
        raise ValueError(
            f"{value!r} is not a post reference; pass at://<did>/app.bsky.feed.post/<rkey> "
            "or https://bsky.app/profile/<handle>/post/<rkey>"
        )
    uri = api.canonical_uri(value, dids={})
    if uri is None:
        raise ValueError(f"the handle in {value!r} does not resolve to an account")
    return uri


def feed_rows(_payload: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Feed items as rows, dropping any item that carries no post."""
    return [flat for row in rows if (flat := flatten_feed_item(row)) is not None]


# ---------------------------------------------------------------------------
# Blended lookups
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True, kw_only=True)
class PostRefArgs:
    """A lone post-reference input column."""

    uri: Annotated[str, Arg(0, doc=_POST_REF_DOC)]


class PostFunction(RowTransformFunction[PostRefArgs]):
    """Hydrated posts by reference, batched 25 to a request."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = POST_SCHEMA

    class Meta:
        name = "post"
        description = "One Bluesky post with its current like, repost and reply counts"
        categories = ["posts", "blended"]
        projection_pushdown = True
        tags = docs(
            category="posts",
            result_schema=POST_SCHEMA,
            llm=(
                "A single post's text, author and live engagement counts, by AT-URI or bsky.app "
                "URL. Reach for this to check how a known post is doing, or to hydrate URIs that "
                "another query produced — the `quoted_uri` and `reply_parent_uri` columns, say. It "
                "composes under a correlated LATERAL."
            ),
            md=(
                "One row per post that exists; a deleted or unknown post returns no row.\n\n"
                "### Accepted references\n\n"
                "Either the AT-URI (`at://did:plc:.../app.bsky.feed.post/3k...`) or the web URL "
                "the app shows (`https://bsky.app/profile/<handle>/post/<rkey>`). A handle is "
                "resolved to its DID first, because the batch endpoint behind this answers HTTP "
                "500 to a handle-based URI.\n\n"
                "### Cost under LATERAL\n\n"
                "Posts are fetched 25 per request."
            ),
            example_queries=examples(
                (
                    "Engagement on the official account's latest post",
                    "SELECT text, like_count, repost_count, reply_count FROM bluesky.main.post(("
                    "SELECT uri FROM bluesky.main.author_feed('bsky.app', filter => 'posts_no_replies') "
                    "WHERE reason IS NULL LIMIT 1))",
                ),
                (
                    "Hydrate the posts that search results are quoting",
                    "SELECT q.author_handle, q.text, q.like_count FROM ("
                    "SELECT quoted_uri FROM bluesky.main.search_posts('duckdb') "
                    "WHERE quoted_uri LIKE '%app.bsky.feed.post%' LIMIT 25) s, "
                    "LATERAL bluesky.main.post(s.quoted_uri) q",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT text, like_count, repost_count, reply_count FROM bluesky.main.post(("
                    "SELECT uri FROM bluesky.main.author_feed('bsky.app', filter => 'posts_no_replies') "
                    "WHERE reason IS NULL LIMIT 1))"
                ),
                description="Engagement on the official account's latest post",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[PostRefArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[PostRefArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        inputs = batch.column("uri").to_pylist()
        hint = CacheHint()
        dids: dict[str, str | None] = {}
        with api.open_client() as client:
            wanted = {
                index: uri
                for index, value in enumerate(inputs)
                if value and (uri := api.canonical_uri(str(value), client=client, dids=dids))
            }
            found = {
                str(view.get("uri")): view
                for view in api.posts(list(wanted.values()), client=client, hint=hint)
            }
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        for index, uri in wanted.items():
            if (view := found.get(uri)) is not None:
                rows.append(flatten_post(view))
                parents.append(index)
        emit_fanout(out, params, rows, parents, blended_cache_control(hint))


@dataclass(slots=True, frozen=True, kw_only=True)
class ThreadArgs:
    """``thread(uri)`` with named reply depth and ancestor height."""

    uri: Annotated[str, Arg(0, doc=_POST_REF_DOC)]
    depth: Annotated[
        int, Arg("depth", doc="Levels of replies to include below the post", default=6, ge=0, le=1000)
    ] = 6
    parent_height: Annotated[
        int,
        Arg("parent_height", doc="Levels of ancestors to include above the post", default=80, ge=0, le=1000),
    ] = 80


class ThreadFunction(RowTransformFunction[ThreadArgs]):
    """A post's conversation — its ancestors and its reply tree — one row per post."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = THREAD_SCHEMA

    class Meta:
        name = "thread"
        description = "A Bluesky post's full conversation: ancestors and replies, one row per post"
        categories = ["posts", "blended"]
        projection_pushdown = True
        tags = docs(
            category="posts",
            result_schema=THREAD_SCHEMA,
            llm=(
                "The whole conversation around one post, flattened to one row per post with a "
                "`depth` column: negative for the posts it replies to, 0 for the post itself, "
                "positive for replies. Reach for this to read a discussion, find the most-liked "
                "reply, or trace a reply back to the post that started it."
            ),
            md=(
                "Bluesky returns a thread as a tree; this flattens it to rows ordered "
                "ancestors-first, then the post, then its replies depth-first.\n\n"
                "### Reading `depth`\n\n"
                "`0` is the post you asked about. `-1` is its parent, `-2` the grandparent, and "
                "so on up to the thread root. `1` is a direct reply, `2` a reply to a reply. "
                "`reply_parent_uri` links each row to the row above it in the tree.\n\n"
                "### What is left out\n\n"
                "Deleted posts and posts hidden by a block have no content, so they produce no "
                "row; replies beneath them still appear. Replies deeper than `depth` (default 6) "
                "are not fetched — raise it, up to 1000, for long chains."
            ),
            example_queries=examples(
                (
                    "The most-liked direct replies to the official account's latest post",
                    "SELECT author_handle, text, like_count FROM bluesky.main.thread(("
                    "SELECT uri FROM bluesky.main.author_feed('bsky.app', filter => 'posts_no_replies') "
                    "WHERE reason IS NULL LIMIT 1), depth => 1) "
                    "WHERE depth = 1 ORDER BY like_count DESC LIMIT 10",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT author_handle, text, like_count FROM bluesky.main.thread(("
                    "SELECT uri FROM bluesky.main.author_feed('bsky.app', filter => 'posts_no_replies') "
                    "WHERE reason IS NULL LIMIT 1), depth => 1) "
                    "WHERE depth = 1 ORDER BY like_count DESC LIMIT 10"
                ),
                description="The most-liked direct replies to the official account's latest post",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ThreadArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[ThreadArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        inputs = batch.column("uri").to_pylist()
        hint = CacheHint()
        dids: dict[str, str | None] = {}
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        with api.open_client() as client:
            for index, value in enumerate(inputs):
                if not value:
                    continue
                uri = api.canonical_uri(str(value), client=client, dids=dids)
                if uri is None:
                    continue
                thread = api.post_thread(
                    uri,
                    depth=params.args.depth,
                    parent_height=params.args.parent_height,
                    client=client,
                    hint=hint,
                )
                if thread is None:
                    continue
                flat = flatten_thread(str(value), thread)
                rows.extend(flat)
                parents.extend([index] * len(flat))
        emit_fanout(out, params, rows, parents, blended_cache_control(hint))


# ---------------------------------------------------------------------------
# Paged scans
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True, kw_only=True)
class AuthorFeedArgs:
    """``author_feed(actor)`` with the endpoint's own filter and pin option."""

    actor: Annotated[str, Arg(0, doc="Handle, DID, @handle or bsky.app profile URL")]
    filter: Annotated[
        str,
        Arg(
            "filter",
            doc="Which posts: posts_with_replies (default), posts_no_replies, posts_with_media, "
            "posts_and_author_threads, posts_with_video",
            default="posts_with_replies",
            choices=[
                "posts_with_replies",
                "posts_no_replies",
                "posts_with_media",
                "posts_and_author_threads",
                "posts_with_video",
            ],
        ),
    ] = "posts_with_replies"
    include_pins: Annotated[
        bool, Arg("include_pins", doc="Put the account's pinned post first", default=False)
    ] = False


@init_single_worker
class AuthorFeedFunction(TableFunctionGenerator[AuthorFeedArgs, PagedScanState]):
    """An account's timeline — its posts and reposts, newest first."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = FEED_ITEM_SCHEMA

    class Meta:
        name = "author_feed"
        description = "A Bluesky account's posts and reposts, newest first"
        categories = ["posts"]
        projection_pushdown = True
        tags = docs(
            category="posts",
            result_schema=FEED_ITEM_SCHEMA,
            llm=(
                "Everything one account has posted or reposted, newest first — their profile "
                "timeline. Reach for this to analyse what an account talks about, how its posts "
                "perform, or how often it posts. Reposts are included and marked `reason = "
                "'repost'`; filter `reason IS NULL` for the account's own posts only."
            ),
            md=(
                "The account's profile timeline, as the app shows it.\n\n"
                "### Reposts are in here\n\n"
                "A repost appears as the *original* post (with the original author) and "
                "`reason = 'repost'`. For only the account's own writing, add `WHERE reason IS "
                "NULL`, or `author_did = <their did>`.\n\n"
                "### The `filter` argument\n\n"
                "`posts_no_replies` drops replies at the source; `posts_with_media` and "
                "`posts_with_video` keep only posts with attachments; `posts_and_author_threads` "
                "keeps replies only when they continue the author's own thread.\n\n"
                "### Streaming\n\n"
                "Rows arrive 100 at a time, so a `LIMIT` stops early. It cannot be the inner side "
                "of a correlated `LATERAL`."
            ),
            example_queries=examples(
                (
                    "The official account's 20 most recent original posts",
                    "SELECT created_at, text, like_count FROM bluesky.main.author_feed("
                    "'bsky.app', filter => 'posts_no_replies') WHERE reason IS NULL "
                    "ORDER BY created_at DESC LIMIT 20",
                ),
                (
                    "Which hashtags an account uses most in its recent posts",
                    "SELECT tag, count(*) AS uses FROM (SELECT unnest(hashtags) AS tag FROM ("
                    "SELECT hashtags FROM bluesky.main.author_feed('bsky.app') LIMIT 300)) "
                    "GROUP BY tag ORDER BY uses DESC",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT created_at, text, like_count FROM bluesky.main.author_feed("
                    "'bsky.app', filter => 'posts_no_replies') WHERE reason IS NULL "
                    "ORDER BY created_at DESC LIMIT 20"
                ),
                description="The official account's 20 most recent original posts",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[AuthorFeedArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[AuthorFeedArgs]) -> PagedScanState:
        return PagedScanState()

    @classmethod
    def process(
        cls, params: ProcessParams[AuthorFeedArgs], state: PagedScanState, out: OutputCollector
    ) -> None:
        """Emit one page of the timeline."""
        emit_page(
            params,
            state,
            out,
            method="app.bsky.feed.getAuthorFeed",
            key="feed",
            query={
                "actor": api.normalize_actor(params.args.actor),
                "filter": params.args.filter,
                "includePins": "true" if params.args.include_pins else None,
            },
            flatten=feed_rows,
            actor=params.args.actor,
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class SearchPostsArgs:
    """``search_posts(query)`` with the endpoint's narrowing options as named args.

    Every optional string uses an empty-string sentinel: a ``str | None``
    annotation resolves to the Arrow null type, which the DuckDB extension
    cannot cast a VARCHAR into.
    """

    query: Annotated[str, Arg(0, doc="Search query; supports Bluesky's syntax, e.g. 'from:bsky.app'")]
    sort: Annotated[
        str,
        Arg(
            "sort",
            doc="'latest' (newest first) or 'top' (by engagement)",
            default="latest",
            choices=["latest", "top"],
        ),
    ] = "latest"
    author: Annotated[str, Arg("author", doc="Only posts by this handle or DID", default="")] = ""
    mentions: Annotated[str, Arg("mentions", doc="Only posts mentioning this handle or DID", default="")] = ""
    lang: Annotated[str, Arg("lang", doc="Only posts in this language, e.g. 'en'", default="")] = ""
    domain: Annotated[str, Arg("domain", doc="Only posts linking to this domain", default="")] = ""
    url: Annotated[str, Arg("url", doc="Only posts linking to this URL", default="")] = ""
    tag: Annotated[str, Arg("tag", doc="Only posts with this hashtag (without '#')", default="")] = ""
    since: Annotated[
        str, Arg("since", doc="Only posts at or after this moment, e.g. '2026-09-01'", default="")
    ] = ""
    until: Annotated[
        str, Arg("until", doc="Only posts before this moment, e.g. '2026-09-01T12:00:00Z'", default="")
    ] = ""
    cache_ttl: Annotated[
        int, Arg("cache_ttl", doc="Seconds to cache results (0 = off; search is live)", default=0, ge=0)
    ] = 0


def _search_window(params: ProcessParams[SearchPostsArgs]) -> tuple[str | None, str | None]:
    """``(since, until)`` for ``searchPosts``: explicit arguments, else what a WHERE allows.

    The endpoint filters on its own ``sortAt`` timestamp, which is the *earlier*
    of a post's ``createdAt`` and ``indexedAt``, not on either column alone. So
    a predicate translates only where it is provably no narrower:

    * An upper bound on either column bounds ``sortAt`` too (the minimum of two
      values is at most each of them), so ``until`` takes the tighter of the two.
    * A lower bound on one column says nothing about the minimum — a future-
      dated ``createdAt`` with an older ``indexedAt`` has a ``sortAt`` below it.
      Only when *both* columns are bounded below is ``sortAt`` bounded, by the
      smaller of the two bounds.
    """
    created_low, created_high = datetime_bounds(params, "created_at")
    indexed_low, indexed_high = datetime_bounds(params, "indexed_at")
    since = params.args.since or None
    until = params.args.until or None
    if since is None and created_low is not None and indexed_low is not None:
        since = iso(min(created_low, indexed_low))
    if until is None:
        highs = [h for h in (created_high, indexed_high) if h is not None]
        until = iso(min(highs)) if highs else None
    return since, until


@init_single_worker
class SearchPostsFunction(TableFunctionGenerator[SearchPostsArgs, PagedScanState]):
    """Full-text post search — one page of up to 100 hits.

    ``searchPosts`` hands back a cursor, but answers **HTTP 403** to any
    anonymous request that presents one (verified against ``api.bsky.app``:
    page one 200, the same query with its own cursor 403). Following it would
    turn every search past 100 hits into an error, so the scan stops after the
    first page. It stays a scan rather than a blended function so a future
    authenticated mode could page without changing the function's shape.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = POST_SCHEMA

    class Meta:
        name = "search_posts"
        description = "Full-text search over Bluesky posts"
        categories = ["posts"]
        projection_pushdown = True
        #: Range predicates on created_at / indexed_at become the endpoint's own
        #: since/until, and an author equality becomes `author`. Not exact —
        #: every predicate is still applied to the rows by `build_filtered`.
        filter_pushdown = True
        # Delivers `current_pushdown_filters` to process(). Without it the
        # filters never arrive, while the engine still drops its own filter
        # above the scan.
        auto_apply_filters = True
        tags = docs(
            category="posts",
            result_schema=POST_SCHEMA,
            llm=(
                "Search every public Bluesky post by keyword, newest first (or by engagement "
                "with `sort => 'top'`); returns at most 100 posts per call. This is the way in "
                "when you have a topic rather than an account or a post: what people are "
                "saying about something, who is saying it, and how much it is being shared. "
                "Narrow with `author`, `lang`, `tag`, `domain`, `since` and `until` — or with "
                "an ordinary WHERE on `created_at` / `author_handle`, which is pushed to the API."
            ),
            md=(
                "Bluesky's full-text post search.\n\n"
                "### Query syntax\n\n"
                "The query string supports Bluesky's search operators — `from:handle`, "
                '`mentions:handle`, `lang:en`, `domain:example.com`, `#hashtag`, `"exact '
                'phrase"` — and each also has a named argument.\n\n'
                "### Filters that are pushed to the API\n\n"
                "`WHERE author_handle = '...'` becomes the endpoint's `author` filter. An upper "
                "bound on `created_at` becomes `until`. A lower bound becomes `since` only when "
                "`indexed_at` is bounded too, because the endpoint filters on the earlier of the "
                "two timestamps — pass `since =>` explicitly to narrow by time at the source.\n\n"
                "### At most 100 posts per search\n\n"
                "Bluesky serves the first page of search results anonymously but refuses (HTTP "
                "403) to page further without a login, so each call returns at most 100 posts. "
                "To see more, narrow the search — by `since`/`until` windows, `author`, `lang` or "
                "`tag` — and union several calls. This is a sample of a topic, not a census.\n\n"
                "### Freshness\n\n"
                "Search is served uncached by the origin; pass `cache_ttl => N` to cache."
            ),
            example_queries=examples(
                (
                    "The latest posts mentioning DuckDB",
                    "SELECT created_at, author_handle, text FROM bluesky.main.search_posts('duckdb') "
                    "LIMIT 25",
                ),
                (
                    "The most-engaged posts on a topic this week",
                    "SELECT author_handle, text, like_count, repost_count "
                    "FROM bluesky.main.search_posts('duckdb', sort => 'top', "
                    "since => CAST(current_date - 7 AS VARCHAR)) "
                    "ORDER BY like_count DESC LIMIT 20",
                ),
                (
                    "Who posted most about a topic among the latest 100 matches",
                    "SELECT author_handle, count(*) AS posts FROM bluesky.main.search_posts('duckdb') "
                    "GROUP BY author_handle ORDER BY posts DESC LIMIT 10",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT created_at, author_handle, text FROM bluesky.main.search_posts('duckdb') LIMIT 25"
                ),
                description="The latest posts mentioning DuckDB",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[SearchPostsArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[SearchPostsArgs]) -> PagedScanState:
        return PagedScanState()

    @classmethod
    def process(
        cls, params: ProcessParams[SearchPostsArgs], state: PagedScanState, out: OutputCollector
    ) -> None:
        """Emit one page of search hits."""

        def query() -> dict[str, Any]:
            args = params.args
            since, until = _search_window(params)
            author = args.author or equality(params, "author_handle") or equality(params, "author_did")
            return {
                "q": args.query,
                "sort": args.sort,
                "author": api.normalize_actor(author) if author else None,
                "mentions": api.normalize_actor(args.mentions) if args.mentions else None,
                "lang": args.lang or None,
                "domain": args.domain or None,
                "url": args.url or None,
                "tag": args.tag.removeprefix("#") or None,
                "since": since,
                "until": until,
            }

        emit_page(
            params,
            state,
            out,
            method="app.bsky.feed.searchPosts",
            key="posts",
            query=query,
            flatten=lambda _payload, rows: [flatten_post(row) for row in rows],
            base=api.search_url(),
            opt_in_ttl=params.args.cache_ttl,
            first_page_only=True,
        )


_INTERACTION_MD = (
    "### The post must be a literal\n\n"
    "Pass the post as a literal AT-URI or bsky.app URL. DuckDB does not accept a subquery as "
    "this function's argument, and because it streams it cannot be the inner side of a "
    "`LATERAL` either — look the URI up first, then call this with it.\n\n"
    "### Streaming\n\n"
    "Rows arrive 100 at a time, most recent first, so a `LIMIT` stops early. For just the number, "
    "`post()` already carries the count."
)


@init_single_worker
class LikesFunction(TableFunctionGenerator[PostRefArgs, PagedScanState]):
    """Accounts that liked a post, most recent first."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = INTERACTION_SCHEMA

    class Meta:
        name = "likes"
        description = "Accounts that liked a Bluesky post, most recent first"
        categories = ["interactions"]
        projection_pushdown = True
        tags = docs(
            category="interactions",
            result_schema=INTERACTION_SCHEMA,
            llm=(
                "Who liked a post, and when. Reach for this to see the audience a post reached or "
                "how quickly it was liked. `interacted_at` is the time of each like. For just the "
                "total, use `post().like_count`."
            ),
            md="Accounts that liked the given post.\n\n" + _INTERACTION_MD,
            example_queries=examples(
                (
                    "Who most recently liked bsky.app's pinned welcome post",
                    f"SELECT handle, interacted_at FROM bluesky.main.likes('{WELCOME_POST}') LIMIT 50",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(f"SELECT handle, interacted_at FROM bluesky.main.likes('{WELCOME_POST}') LIMIT 50"),
                description="Who most recently liked bsky.app's pinned welcome post",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[PostRefArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[PostRefArgs]) -> PagedScanState:
        return PagedScanState()

    @classmethod
    def process(cls, params: ProcessParams[PostRefArgs], state: PagedScanState, out: OutputCollector) -> None:
        """Emit one page of likers."""
        emit_page(
            params,
            state,
            out,
            method="app.bsky.feed.getLikes",
            key="likes",
            query=lambda: {"uri": _require_post_uri(params.args.uri)},
            flatten=lambda payload, rows: [flatten_like(str(payload.get("uri")), row) for row in rows],
        )


@init_single_worker
class RepostedByFunction(TableFunctionGenerator[PostRefArgs, PagedScanState]):
    """Accounts that reposted a post."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = INTERACTION_SCHEMA

    class Meta:
        name = "reposted_by"
        description = "Accounts that reposted a Bluesky post"
        categories = ["interactions"]
        projection_pushdown = True
        tags = docs(
            category="interactions",
            result_schema=INTERACTION_SCHEMA,
            llm=(
                "Who reposted (boosted) a post. Reach for this to see who spread a post to their "
                "own followers. The endpoint does not timestamp reposts, so `interacted_at` is "
                "NULL. For just the total, use `post().repost_count`; for quote posts, `quotes()`."
            ),
            md="Accounts that reposted the given post.\n\n" + _INTERACTION_MD,
            example_queries=examples(
                (
                    "Who reposted bsky.app's pinned welcome post",
                    f"SELECT handle, display_name FROM bluesky.main.reposted_by('{WELCOME_POST}') LIMIT 50",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(f"SELECT handle, display_name FROM bluesky.main.reposted_by('{WELCOME_POST}') LIMIT 50"),
                description="Who reposted bsky.app's pinned welcome post",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[PostRefArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[PostRefArgs]) -> PagedScanState:
        return PagedScanState()

    @classmethod
    def process(cls, params: ProcessParams[PostRefArgs], state: PagedScanState, out: OutputCollector) -> None:
        """Emit one page of reposters."""
        emit_page(
            params,
            state,
            out,
            method="app.bsky.feed.getRepostedBy",
            key="repostedBy",
            query=lambda: {"uri": _require_post_uri(params.args.uri)},
            flatten=lambda payload, rows: [flatten_reposter(str(payload.get("uri")), row) for row in rows],
        )


@init_single_worker
class QuotesFunction(TableFunctionGenerator[PostRefArgs, PagedScanState]):
    """Posts that quote a post."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = POST_SCHEMA

    class Meta:
        name = "quotes"
        description = "Posts quoting a Bluesky post, most recent first"
        categories = ["interactions"]
        projection_pushdown = True
        tags = docs(
            category="interactions",
            result_schema=POST_SCHEMA,
            llm=(
                "Every post that quotes a given post, with its text and engagement. Reach for this "
                "to see what people said *about* a post, as opposed to replies (use `thread()`) or "
                "plain reposts (use `reposted_by()`). Each row's `quoted_uri` is the input post."
            ),
            md="Quote posts of the given post, as full post rows.\n\n" + _INTERACTION_MD,
            example_queries=examples(
                (
                    "The most-liked quotes of bsky.app's pinned welcome post",
                    f"SELECT author_handle, text, like_count FROM bluesky.main.quotes('{WELCOME_POST}') "
                    "ORDER BY like_count DESC LIMIT 20",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT author_handle, text, like_count FROM bluesky.main.quotes('{WELCOME_POST}') "
                    "ORDER BY like_count DESC LIMIT 20"
                ),
                description="The most-liked quotes of bsky.app's pinned welcome post",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[PostRefArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[PostRefArgs]) -> PagedScanState:
        return PagedScanState()

    @classmethod
    def process(cls, params: ProcessParams[PostRefArgs], state: PagedScanState, out: OutputCollector) -> None:
        """Emit one page of quote posts."""
        emit_page(
            params,
            state,
            out,
            method="app.bsky.feed.getQuotes",
            key="posts",
            query=lambda: {"uri": _require_post_uri(params.args.uri)},
            flatten=lambda _payload, rows: [flatten_post(row) for row in rows],
        )


POST_FUNCTIONS: list[type] = [
    PostFunction,
    ThreadFunction,
    AuthorFeedFunction,
    SearchPostsFunction,
    LikesFunction,
    RepostedByFunction,
    QuotesFunction,
]
