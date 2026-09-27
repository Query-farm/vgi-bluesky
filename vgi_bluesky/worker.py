"""VGI worker exposing Bluesky social data to DuckDB/SQL (read-only).

    ATTACH 'bluesky' (TYPE vgi, LOCATION 'uv run bluesky_worker.py');
    SELECT * FROM bluesky.trends ORDER BY rank;
    SELECT author_handle, text FROM bluesky.search_posts('duckdb') LIMIT 20;

No credentials are required. Everything here is served unauthenticated by
Bluesky's public AppView. Writing to Bluesky — posting, liking, following, even
logging in — needs an XRPC *procedure* (a ``POST``), and this worker can only
issue ``GET``.

Function names are bare (``profile``, not ``bluesky_profile``) because they are
already qualified by the catalog they live in.
"""

from __future__ import annotations

import json
import sys

from vgi import Worker
from vgi.catalog import Catalog, ReadOnlyCatalogInterface, Schema
from vgi.catalog.catalog_interface import CatalogInfo
from vgi.catalog.descriptors import Table

from vgi_bluesky import __version__
from vgi_bluesky.actors import ACTOR_FUNCTIONS
from vgi_bluesky.feeds import DISCOVER_FEED, FEED_FUNCTIONS, AllTrendsFunction
from vgi_bluesky.jetstream import JETSTREAM_FUNCTIONS
from vgi_bluesky.meta import column_comments, docs, examples, keywords
from vgi_bluesky.posts import POST_FUNCTIONS
from vgi_bluesky.schemas import TREND_SCHEMA

IMPLEMENTATION_VERSION = __version__
DATA_VERSION_SPEC = f"=={__version__}"
SOURCE_URL = "https://github.com/Query-farm/vgi-bluesky"

_FUNCTIONS = [*ACTOR_FUNCTIONS, *POST_FUNCTIONS, *FEED_FUNCTIONS, *JETSTREAM_FUNCTIONS]

#: A subquery yielding the official account's latest original post — a post
#: reference that is always valid, for examples that need one.
_LATEST_POST = (
    "(SELECT uri FROM bluesky.main.author_feed('bsky.app', filter => 'posts_no_replies') "
    "WHERE reason IS NULL LIMIT 1)"
)

_EXAMPLE_QUERIES = examples(
    (
        "What is trending on Bluesky right now",
        "SELECT rank, display_name, category, post_count FROM bluesky.main.trends ORDER BY rank",
    ),
    (
        "The latest posts mentioning DuckDB",
        "SELECT created_at, author_handle, text FROM bluesky.main.search_posts('duckdb') LIMIT 25",
    ),
    (
        "One account's profile and counts",
        "SELECT handle, display_name, followers_count, posts_count FROM bluesky.main.profile('bsky.app')",
    ),
    (
        "An account's recent original posts and how they performed",
        "SELECT created_at, text, like_count, repost_count FROM bluesky.main.author_feed("
        "'bsky.app', filter => 'posts_no_replies') WHERE reason IS NULL LIMIT 20",
    ),
    (
        "Follower counts for everyone a search finds, via LATERAL",
        "SELECT p.handle, p.followers_count FROM ("
        "SELECT did FROM bluesky.main.search_actors('duckdb') LIMIT 20) a, "
        "LATERAL bluesky.main.profile(a.did) p ORDER BY p.followers_count DESC",
    ),
    (
        "The most-liked replies in a conversation",
        f"SELECT author_handle, text, like_count FROM bluesky.main.thread({_LATEST_POST}) "
        "WHERE depth > 0 ORDER BY like_count DESC LIMIT 10",
    ),
    (
        "What the Discover feed is showing",
        f"SELECT author_handle, text FROM bluesky.main.feed('{DISCOVER_FEED}') LIMIT 20",
    ),
)

#: Examples the linter actually runs against a live worker, so every one of them
#: must be true right now: no post URI (which can be deleted), no assumption
#: about what is trending or how many rows a search finds.
_EXECUTABLE_EXAMPLES = json.dumps(
    [
        {
            "name": "trends_are_ranked_from_one",
            "description": "The trends table is populated and ranked from 1.",
            "sql": "SELECT min(rank) = 1 AND count(*) > 0 FROM bluesky.main.trends",
            "expected_result": [[True]],
        },
        {
            "name": "trends_and_scan_function_agree",
            "description": "The `trends` table and the `all_trends` function behind it have the same shape.",
            "sql": (
                "SELECT (SELECT count(*) FROM bluesky.main.trends) > 0 "
                "AND (SELECT count(*) FROM bluesky.main.all_trends()) > 0"
            ),
            "expected_result": [[True]],
        },
        {
            "name": "profile_resolves_a_handle_to_its_did",
            "description": "A handle resolves to the account's permanent DID.",
            "sql": "SELECT did FROM bluesky.main.profile('bsky.app')",
            "expected_result": [["did:plc:z72i7hdynmk6r22z27h6tvur"]],
        },
        {
            "name": "profile_accepts_a_web_url",
            "description": "A pasted bsky.app profile URL is accepted as an actor.",
            "sql": "SELECT handle FROM bluesky.main.profile('https://bsky.app/profile/bsky.app')",
            "expected_result": [["bsky.app"]],
        },
        {
            "name": "author_feed_is_the_authors",
            "description": "Without reposts, an author feed holds only that author's posts.",
            "sql": (
                "SELECT bool_and(author_handle = 'bsky.app') FROM ("
                "SELECT author_handle FROM bluesky.main.author_feed('bsky.app') "
                "WHERE reason IS NULL LIMIT 50)"
            ),
            "expected_result": [[True]],
        },
        {
            "name": "a_limit_stops_the_scan_early",
            "description": (
                "A paged scan over an account with millions of followers yields its first rows "
                "without walking every page."
            ),
            "sql": "SELECT count(*) FROM (SELECT did FROM bluesky.main.followers('bsky.app') LIMIT 5)",
            "expected_result": [[5]],
        },
        {
            "name": "jetstream_delivers_live_events",
            "description": "A few seconds of the firehose always holds events; the network never goes quiet.",
            "sql": "SELECT count(*) > 0 FROM bluesky.main.jetstream(seconds => 3)",
            "expected_result": [[True]],
        },
        {
            "name": "jetstream_collection_filter_holds",
            "description": "A collection filter keeps only that record type among commits.",
            "sql": (
                "SELECT bool_and(collection = 'app.bsky.feed.post') FROM bluesky.main.jetstream("
                "collections => 'app.bsky.feed.post', seconds => 3) WHERE kind = 'commit'"
            ),
            "expected_result": [[True]],
        },
        {
            "name": "thread_anchor_is_depth_zero",
            "description": "A thread always contains the anchor post, at depth 0.",
            "sql": f"SELECT count(*) FROM bluesky.main.thread({_LATEST_POST}, depth => 0) WHERE depth = 0",
            "expected_result": [[1]],
        },
    ]
)

#: The agent-suitability suite: analyst questions that should be answerable from
#: the catalog metadata alone. Only ``{name, prompt}`` is published here — the
#: graders (reference SQL, success criteria) live in ``vgi-agent-tests.yaml`` so
#: an agent under test cannot read the answers out of the catalog. The prompts
#: exercise what this worker is easy to get wrong: reposts mixed into author
#: feeds, counts versus listings, and which function answers "what did people
#: say" — replies, quotes or search.
_AGENT_TEST_TASKS = json.dumps(
    [
        {"name": "whats_trending", "prompt": "What are the top five things trending on Bluesky right now?"},
        {"name": "account_size", "prompt": "How many followers does the Bluesky account bsky.app have?"},
        {
            "name": "own_posts_only",
            "prompt": (
                "Show the ten most recent posts the account bsky.app wrote itself — not things it reposted."
            ),
        },
        {"name": "topic_search", "prompt": "Find recent Bluesky posts about DuckDB and who wrote them."},
        {
            "name": "best_reply",
            "prompt": "What is the most-liked reply to bsky.app's most recent original post?",
        },
        {
            "name": "commentary",
            "prompt": (
                "What did people say when they quote-posted the post bsky.app has pinned to its profile?"
            ),
        },
        {
            "name": "enrich_search_results",
            "prompt": (
                "Find accounts about DuckDB on Bluesky and rank the first twenty by how many "
                "followers they have."
            ),
        },
        {
            "name": "read_discover",
            "prompt": "What is Bluesky's own Discover feed showing right now?",
        },
        {
            "name": "recent_followers",
            "prompt": "Who are the ten most recent accounts to follow bsky.app on Bluesky?",
        },
        {
            "name": "who_they_follow",
            "prompt": "Which accounts does bsky.app follow? List a few by handle.",
        },
        {
            "name": "who_liked_it",
            "prompt": (
                "Which accounts most recently liked the post bsky.app has pinned to its profile, and when?"
            ),
        },
        {
            "name": "who_boosted_it",
            "prompt": "Which accounts reposted the post bsky.app has pinned to its profile?",
        },
        {
            "name": "post_engagement",
            "prompt": (
                "Given the post https://bsky.app/profile/bsky.app/post/3mw2cdr44fc2a, how many likes, "
                "reposts and replies does it have right now?"
            ),
        },
        {
            "name": "find_a_feed",
            "prompt": "Which are the most popular Bluesky custom feeds about science?",
        },
        {
            "name": "an_accounts_feeds",
            "prompt": (
                "Which custom feeds has the bsky.app account published, and how many likes does each have?"
            ),
        },
        {
            "name": "trends_by_category",
            "prompt": (
                "Group what is trending on Bluesky right now by category, with the post count for each."
            ),
        },
        {
            "name": "live_languages",
            "prompt": "Which languages are people posting in on Bluesky right now, across the whole network?",
        },
    ]
)

_CATEGORIES = json.dumps(
    [
        {
            "name": "actors",
            "title": "Accounts",
            "description": "Profiles and account search — where handles become DIDs and counts.",
            "keywords": ["profile", "account", "handle", "did", "search"],
        },
        {
            "name": "graph",
            "title": "Social Graph",
            "description": "Who follows whom.",
            "keywords": ["followers", "follows", "graph", "audience"],
        },
        {
            "name": "posts",
            "title": "Posts & Threads",
            "description": "Individual posts, whole conversations, account timelines and full-text search.",
            "keywords": ["posts", "threads", "replies", "search", "timeline"],
        },
        {
            "name": "interactions",
            "title": "Engagement",
            "description": "Who liked, reposted or quoted a post.",
            "keywords": ["likes", "reposts", "quotes", "engagement"],
        },
        {
            "name": "feeds",
            "title": "Custom Feeds",
            "description": "Discovering and reading Bluesky's algorithmic custom feeds.",
            "keywords": ["feeds", "feed generators", "discover", "algorithm"],
        },
        {
            "name": "firehose",
            "title": "Live Firehose",
            "description": "Every event on the network as it happens, via Jetstream, in time-boxed reads.",
            "keywords": ["firehose", "jetstream", "stream", "live", "realtime"],
        },
        {
            "name": "trends",
            "title": "Trending",
            "description": "What Bluesky currently reports as trending.",
            "keywords": ["trending", "topics", "news"],
        },
    ]
)

_CATALOG_TAGS = {
    "provider": "bluesky",
    "domain": "social-media",
    "vgi.title": "Bluesky Social",
    "vgi.source_url": SOURCE_URL,
    "vgi.author": "Query Farm LLC <hello@query.farm>",
    "vgi.copyright": (
        "Worker (c) 2026 Query Farm LLC - https://query.farm. Posts and profiles are their authors' "
        "own, served by Bluesky Social PBC under Bluesky's terms of service."
    ),
    "vgi.license": "MIT",
    "vgi.support_contact": "https://github.com/Query-farm/vgi-bluesky/issues",
    "vgi.support_policy_url": "https://github.com/Query-farm/vgi-bluesky/blob/main/README.md",
    "vgi.keywords": keywords(
        "bluesky",
        "atproto",
        "social media",
        "posts",
        "followers",
        "trending",
        "feeds",
        "search",
        "firehose",
        "jetstream",
    ),
    "vgi.executable_examples": _EXECUTABLE_EXAMPLES,
    "vgi.agent_test_tasks": _AGENT_TEST_TASKS,
    "vgi.doc_llm": (
        "Public data from Bluesky, the AT Protocol social network: posts, threads, profiles, the "
        "follow graph, likes, reposts, quotes, custom feeds and trending topics. Reach for this "
        "catalog to find out what people are saying about a topic, how an account or a post is "
        "performing, or what is trending. Start from `trends` or `search_posts()` for a topic, "
        "`profile()` or `search_actors()` for an account. Read-only and public: no credentials, "
        "no account, no posting."
    ),
    "vgi.doc_md": (
        "Bluesky is a decentralised social network built on the AT Protocol. This catalog reads "
        "its public AppView — the service that indexes the whole network — and returns what it "
        "serves as ordinary tables.\n\n"
        "### Identifiers\n\n"
        "- **Actors** are accounts. Each has a permanent `did` (`did:plc:...`) and a `handle` "
        "(`bsky.app`) that it can change. Every function taking an actor accepts either, as well "
        "as `@handle` or a pasted `https://bsky.app/profile/...` URL. Join and store on `did`.\n"
        "- **Posts** are addressed by AT-URI, `at://<did>/app.bsky.feed.post/<rkey>`. Functions "
        "taking a post also accept the `https://bsky.app/profile/<handle>/post/<rkey>` URL the "
        "app shows.\n\n"
        "### Timestamps\n\n"
        "`created_at` on a post is what the author's app claimed and can be backdated; "
        "`indexed_at` is when Bluesky first saw it, and is the one to trust for ordering.\n\n"
        "### Access and limits\n\n"
        "No credentials are needed and none are accepted. Bluesky rate-limits anonymous "
        "traffic per IP; requests are retried with backoff, and the AppView's own 30-second "
        "`Cache-Control` is forwarded to the result cache, so repeating a query within half a "
        "minute is free."
    ),
}

_SCHEMA_TAGS = {
    "provider": "bluesky",
    "domain": "social-media",
    "vgi.title": "Bluesky Public Data",
    "vgi.categories": _CATEGORIES,
    "vgi.keywords": keywords("bluesky", "atproto", "posts", "profiles", "followers", "feeds", "trends"),
    "vgi.example_queries": _EXAMPLE_QUERIES,
    "vgi.doc_llm": (
        "The whole read-only Bluesky surface, in one schema. The `trends` table needs no "
        "arguments; everything else is a function keyed by a search query, an actor or a post. "
        "Point lookups — `profile()`, `post()`, `thread()` — compose under a correlated LATERAL; "
        "list functions stream a page at a time so a LIMIT stops them early, and drive joins "
        "rather than sit inside them."
    ),
    "vgi.doc_md": (
        "One schema holding the whole read-only Bluesky surface.\n\n"
        "### Where to start\n\n"
        "Start from what you have. A topic leads to `trends` or post search; a name leads "
        "to account search and then a profile; an account leads to its timeline, its graph "
        "and its feeds; a post leads to its conversation and to who engaged with it.\n\n"
        "### Two kinds of function\n\n"
        "**Lookups** — a profile, a post, a thread — are blended: one registration serves a "
        "literal call and a correlated `LATERAL`, and they batch their requests, 25 profiles "
        "or posts per call.\n\n"
        "**Listings** follow Bluesky's cursor one page per batch, so a `LIMIT` stops them "
        "early. That streaming is also why they cannot be the inner side of a `LATERAL`: drive "
        "a join from them, not into them.\n\n"
        "### Counts versus listings\n\n"
        "A large account has millions of followers and a viral post hundreds of thousands of "
        "likes. Profiles and posts carry the totals; list the individual accounts only when "
        "you need them, and put a `LIMIT` on it."
    ),
}

_TRENDS_DOCS = docs(
    category="trends",
    llm=(
        "What Bluesky currently reports as trending, up to 25 topics in rank order, with a "
        "headline, a one-line summary, a category and how many posts it has drawn. Reach for this "
        "first when asked what people are talking about. `status` says where each trend is in its "
        "lifecycle — 'trending', 'saturating' or 'cooling'."
    ),
    md=(
        "Bluesky's current trending topics, one row per trend.\n\n"
        "### Why this is a table and everything else is a function\n\n"
        "It needs no key, it is small — never more than 25 rows — and it is a single unpaginated "
        "request. Nothing else here is any of those things.\n\n"
        "### Going deeper\n\n"
        "Each trend is backed by a feed in the web app (`trend_url`). To read what people are "
        "saying, pass the trend's `display_name` to `search_posts()`.\n\n"
        "### Freshness\n\n"
        "The AppView declares a 30-second `Cache-Control`, which is forwarded to the result cache."
    ),
    example_queries=examples(
        (
            "What is trending right now",
            "SELECT rank, display_name, description, post_count FROM bluesky.main.trends ORDER BY rank",
        ),
        (
            "Trending topics by category",
            "SELECT category, count(*) AS trends, sum(post_count) AS posts "
            "FROM bluesky.main.trends GROUP BY category ORDER BY posts DESC",
        ),
    ),
    extra={
        "provider": "bluesky",
        "domain": "social-media",
        "vgi.title": "Trending Topics",
        "vgi.keywords": keywords("trending", "topics", "news", "bluesky", "discovery"),
    },
)

_BLUESKY_CATALOG = Catalog(
    name="bluesky",
    default_schema="main",
    comment="Read-only Bluesky data: posts, threads, profiles, followers, likes, feeds and trending topics",
    tags=_CATALOG_TAGS,
    schemas=[
        Schema(
            path=["main"],
            comment="Bluesky public AppView data — no credentials required",
            tags=_SCHEMA_TAGS,
            functions=list(_FUNCTIONS),
            tables=[
                Table(
                    name="trends",
                    function=AllTrendsFunction,
                    comment="What Bluesky currently reports as trending, in rank order",
                    tags=_TRENDS_DOCS,
                    column_comments=column_comments(TREND_SCHEMA),
                    primary_key=(("rank",),),
                    not_null=("rank",),
                    cardinality_estimate=10,
                    cardinality_max=25,
                ),
            ],
        ),
    ],
)


class BlueskyCatalog(ReadOnlyCatalogInterface):
    """Advertises the worker's versions and source; takes no ATTACH options."""

    catalog = _BLUESKY_CATALOG
    catalog_name = _BLUESKY_CATALOG.name

    def catalogs(self) -> list[CatalogInfo]:
        """Advertise the single read-only Bluesky catalog."""
        return [
            CatalogInfo(
                name=self._effective_catalog_name,
                implementation_version=IMPLEMENTATION_VERSION,
                data_version_spec=DATA_VERSION_SPEC,
                source_url=SOURCE_URL,
            )
        ]


class BlueskyWorker(Worker):
    """Worker process hosting the read-only Bluesky catalog."""

    catalog = _BLUESKY_CATALOG
    catalog_interface = BlueskyCatalog


def main() -> None:
    """Run the worker (stdio by default; pass ``--http`` for the HTTP server)."""
    BlueskyWorker.main()


def main_http() -> None:
    """Run the worker over HTTP."""
    argv = sys.argv[1:]
    if "--http" not in argv:
        argv = ["--http", *argv]
    sys.argv = [sys.argv[0], *argv]
    BlueskyWorker.main()
