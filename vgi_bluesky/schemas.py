"""Arrow schemas, and the flattening of Bluesky's nested views into rows.

The AppView returns deeply nested *views*: a post view wraps an author view, a
free-form ``record`` (the post as its author wrote it), an ``embed`` view, and
counters. Each function here flattens one kind of view into a flat ``dict``
keyed by the column names of the matching schema, and :func:`batch_from_rows`
turns those dicts into an Arrow batch.

Every conversion is **total**. A post record is authored by an arbitrary client
and only loosely validated — ``createdAt`` in particular is whatever the posting
app wrote — so a value that cannot be represented becomes NULL rather than
failing the batch it arrived in.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa

from vgi_bluesky.meta import field

#: Bluesky timestamps are RFC 3339 / ISO 8601, usually with a trailing ``Z``.
TIMESTAMP = pa.timestamp("us", tz="UTC")

#: The window a nanosecond-resolution consumer can hold. Arrow stores these at
#: microsecond resolution, but pandas and anything casting to ``timestamp[ns]``
#: are limited to 1677-2262 and raise ``OverflowError`` when materializing a
#: value outside it. Post ``createdAt`` values are client-authored and do fall
#: outside it, so they become NULL rather than an unreadable result.
_NS_FLOOR = datetime(1678, 1, 1, tzinfo=UTC)
_NS_CEILING = datetime(2262, 1, 1, tzinfo=UTC)

#: Where a post, profile or feed lives in the Bluesky web app.
WEB_BASE = "https://bsky.app"


# ---------------------------------------------------------------------------
# Total value conversions
# ---------------------------------------------------------------------------


def to_timestamp(value: Any) -> datetime | None:
    """Parse an ISO 8601 string (or pass through a datetime) as an aware UTC datetime, or None.

    Anything unparseable or outside what a nanosecond-resolution client can hold
    becomes NULL. A naive timestamp is read as UTC, which is what the AT
    Protocol's ``datetime`` format requires of it anyway.
    """
    if isinstance(value, datetime):
        parsed = value
    elif value is None or value == "" or not isinstance(value, str):
        return None
    else:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, OverflowError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    try:
        if not _NS_FLOOR <= parsed <= _NS_CEILING:
            return None
    except OverflowError:  # pragma: no cover - aware comparison of an extreme offset
        return None
    return parsed.astimezone(UTC)


def to_integer(value: Any) -> int | None:
    """Parse an integer, or None when the value is not one."""
    if value is None or isinstance(value, bool) or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _nullable_nested(value: Any, kind: pa.DataType) -> Any:
    """``value`` if Arrow accepts it alone, else None."""
    if value is None:
        return None
    try:
        pa.array([value], type=kind)
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError, OverflowError):
        return None
    return value


def column(rows: Sequence[dict[str, Any]], key: str, f: pa.Field) -> pa.Array:
    """Extract ``key`` from every row and build the Arrow array ``f`` declares.

    The conversion is chosen from the field's declared type, so a schema edit is
    the only thing needed to change how a column is parsed. Every branch is
    total: one malformed value must not cost every other row in the batch.
    """
    values = [row.get(key) for row in rows]
    if pa.types.is_timestamp(f.type):
        return pa.array([to_timestamp(v) for v in values], type=f.type)
    if pa.types.is_boolean(f.type):
        return pa.array([None if v is None else bool(v) for v in values], type=f.type)
    if pa.types.is_integer(f.type):
        return pa.array([to_integer(v) for v in values], type=f.type)
    if pa.types.is_string(f.type):
        return pa.array([None if v is None else str(v) for v in values], type=f.type)
    try:
        return pa.array(values, type=f.type)
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError, OverflowError):
        return pa.array([_nullable_nested(v, f.type) for v in values], type=f.type)


def batch_from_rows(rows: Sequence[dict[str, Any]], schema: pa.Schema) -> pa.RecordBatch:
    """Build one RecordBatch by pulling each schema field out of ``rows`` by name."""
    return pa.RecordBatch.from_arrays([column(rows, f.name, f) for f in schema], schema=schema)


# ---------------------------------------------------------------------------
# Small accessors over loosely-typed JSON
# ---------------------------------------------------------------------------


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _strings(values: Iterable[Any]) -> list[str]:
    """Distinct non-empty strings, in first-seen order."""
    return list(dict.fromkeys(str(v) for v in values if isinstance(v, str) and v))


def _labels(view: dict[str, Any]) -> list[str]:
    """The moderation label values applied to a view (``porn``, ``!hide``, ...)."""
    return _strings(_dict(label).get("val") for label in _list(view.get("labels")))


def rkey_of(uri: Any) -> str | None:
    """The record key — the last segment — of an AT-URI."""
    if not isinstance(uri, str) or not uri.startswith("at://"):
        return None
    return uri.rsplit("/", 1)[-1] or None


def _profile_path(did: Any, handle: Any) -> str | None:
    """The web app's path segment for an actor: the handle when valid, else the DID."""
    if isinstance(handle, str) and handle and handle != "handle.invalid":
        return handle
    return did if isinstance(did, str) and did else None


# ---------------------------------------------------------------------------
# Actors
# ---------------------------------------------------------------------------

_ACTOR_FIELDS = [
    field("did", pa.string(), "Permanent account identifier (DID); stable across handle changes."),
    field(
        "handle",
        pa.string(),
        "Current handle, e.g. 'bsky.app'. Can change; 'handle.invalid' means its DNS/HTTP proof failed.",
    ),
    field("display_name", pa.string(), "Free-form display name the account chose."),
    field("description", pa.string(), "Profile bio text."),
    field("avatar_url", pa.string(), "CDN URL of the avatar image."),
    field("created_at", TIMESTAMP, "When the account's profile was created."),
    field("indexed_at", TIMESTAMP, "When the AppView last indexed this profile."),
    field(
        "labels",
        pa.list_(pa.string()),
        "Moderation labels applied to the account, e.g. '!no-unauthenticated'.",
    ),
    field("profile_url", pa.string(), "Link to the profile in the Bluesky web app."),
]

#: A basic actor view: search results, followers, follows, likers, reposters.
ACTOR_SCHEMA = pa.schema(_ACTOR_FIELDS)


def flatten_actor(view: dict[str, Any]) -> dict[str, Any]:
    """A basic actor view as one row of :data:`ACTOR_SCHEMA`."""
    did, handle = view.get("did"), view.get("handle")
    path = _profile_path(did, handle)
    return {
        "did": did,
        "handle": handle,
        "display_name": view.get("displayName"),
        "description": view.get("description"),
        "avatar_url": view.get("avatar"),
        "created_at": view.get("createdAt"),
        "indexed_at": view.get("indexedAt"),
        "labels": _labels(view),
        "profile_url": f"{WEB_BASE}/profile/{path}" if path else None,
    }


PROFILE_SCHEMA = pa.schema(
    [
        field(
            "actor",
            pa.string(),
            "The actor exactly as it was passed in — a handle, DID or profile URL — so a LATERAL can "
            "join back on its input.",
        ),
        *_ACTOR_FIELDS,
        field("banner_url", pa.string(), "CDN URL of the profile banner image."),
        field("pronouns", pa.string(), "Pronouns the account lists on its profile."),
        field("website", pa.string(), "Website the account lists on its profile."),
        field("followers_count", pa.int64(), "Accounts following this one."),
        field("follows_count", pa.int64(), "Accounts this one follows."),
        field("posts_count", pa.int64(), "Posts this account has made (including replies)."),
        field("lists_count", pa.int64(), "Lists this account has created."),
        field("feeds_count", pa.int64(), "Custom feeds this account has published."),
        field("starter_packs_count", pa.int64(), "Starter packs this account has created."),
        field("is_labeler", pa.bool_(), "Whether this account runs a moderation labeling service."),
        field(
            "verified_status",
            pa.string(),
            "Bluesky's blue-check verification state: 'valid', 'invalid' or 'none'.",
        ),
    ]
)


def flatten_profile(actor: str, view: dict[str, Any]) -> dict[str, Any]:
    """A detailed profile view as one row of :data:`PROFILE_SCHEMA`."""
    associated = _dict(view.get("associated"))
    return {
        "actor": actor,
        **flatten_actor(view),
        "banner_url": view.get("banner"),
        "pronouns": view.get("pronouns"),
        "website": view.get("website"),
        "followers_count": view.get("followersCount"),
        "follows_count": view.get("followsCount"),
        "posts_count": view.get("postsCount"),
        "lists_count": associated.get("lists"),
        "feeds_count": associated.get("feedgens"),
        "starter_packs_count": associated.get("starterPacks"),
        "is_labeler": associated.get("labeler"),
        "verified_status": _dict(view.get("verification")).get("verifiedStatus"),
    }


#: Followers / follows: the listed account, plus whose graph it was listed from.
GRAPH_SCHEMA = pa.schema(
    [
        field("subject_did", pa.string(), "DID of the account whose followers or follows these are."),
        field("subject_handle", pa.string(), "Handle of that account."),
        *_ACTOR_FIELDS,
    ]
)


def flatten_graph(subject: dict[str, Any], view: dict[str, Any]) -> dict[str, Any]:
    """One follower/follow as a row of :data:`GRAPH_SCHEMA`."""
    return {"subject_did": subject.get("did"), "subject_handle": subject.get("handle"), **flatten_actor(view)}


#: Likers / reposters of one post.
INTERACTION_SCHEMA = pa.schema(
    [
        field("subject_uri", pa.string(), "AT-URI of the post that was liked or reposted."),
        field(
            "interacted_at",
            TIMESTAMP,
            "When the like was made. NULL for reposts, which the endpoint does not timestamp.",
        ),
        *_ACTOR_FIELDS,
    ]
)


def flatten_like(subject_uri: str, like: dict[str, Any]) -> dict[str, Any]:
    """One entry of ``getLikes`` as a row of :data:`INTERACTION_SCHEMA`."""
    return {
        "subject_uri": subject_uri,
        "interacted_at": like.get("createdAt"),
        **flatten_actor(_dict(like.get("actor"))),
    }


def flatten_reposter(subject_uri: str, view: dict[str, Any]) -> dict[str, Any]:
    """One entry of ``getRepostedBy`` as a row of :data:`INTERACTION_SCHEMA`."""
    return {"subject_uri": subject_uri, "interacted_at": None, **flatten_actor(view)}


# ---------------------------------------------------------------------------
# Posts
# ---------------------------------------------------------------------------

_POST_FIELDS = [
    field(
        "uri",
        pa.string(),
        "AT-URI of the post (at://<did>/app.bsky.feed.post/<rkey>); the key post() and thread() take.",
    ),
    field("cid", pa.string(), "Content hash of this exact version of the post record."),
    field("author_did", pa.string(), "DID of the author."),
    field("author_handle", pa.string(), "Handle of the author at the time of the query."),
    field("author_display_name", pa.string(), "Display name of the author."),
    field("text", pa.string(), "The post's text."),
    field(
        "created_at",
        TIMESTAMP,
        "When the author's client says the post was written. Client-supplied, so it can be "
        "backdated; NULL when unrepresentable.",
    ),
    field("indexed_at", TIMESTAMP, "When the AppView first saw the post — the trustworthy clock."),
    field("langs", pa.list_(pa.string()), "BCP-47 language tags the author's client declared, e.g. ['en']."),
    field("reply_count", pa.int64(), "Replies to this post."),
    field("repost_count", pa.int64(), "Reposts of this post."),
    field("like_count", pa.int64(), "Likes of this post."),
    field("quote_count", pa.int64(), "Quote posts of this post."),
    field("bookmark_count", pa.int64(), "Times this post has been bookmarked."),
    field("reply_parent_uri", pa.string(), "AT-URI of the post this replies to; NULL for a top-level post."),
    field("reply_root_uri", pa.string(), "AT-URI of the first post in the thread this reply belongs to."),
    field(
        "embed_type",
        pa.string(),
        "What is attached: 'images', 'video', 'external' (link card), 'record' (quote post), "
        "'recordWithMedia', or NULL for plain text.",
    ),
    field("quoted_uri", pa.string(), "AT-URI of the quoted post or feed, when this is a quote post."),
    field("external_url", pa.string(), "URL of the attached link card, when there is one."),
    field("image_count", pa.int64(), "Number of attached images (0 when none)."),
    field("hashtags", pa.list_(pa.string()), "Hashtags in the text or the post's tag list, without the '#'."),
    field("mentions", pa.list_(pa.string()), "DIDs of accounts mentioned in the text."),
    field("links", pa.list_(pa.string()), "URLs linked from the text."),
    field("labels", pa.list_(pa.string()), "Moderation labels applied to the post."),
    field("post_url", pa.string(), "Link to the post in the Bluesky web app."),
]

POST_SCHEMA = pa.schema(_POST_FIELDS)

#: ``$type`` prefix shared by every embed record type.
_EMBED_PREFIX = "app.bsky.embed."


def _embed_facts(embed: dict[str, Any]) -> dict[str, Any]:
    """``embed_type``, ``quoted_uri``, ``external_url`` and ``image_count`` from a record embed.

    Read from the *record's* embed (what the author attached) rather than the
    hydrated embed view, because the record's shape is the lexicon's and the
    view's is presentation. ``recordWithMedia`` nests both a quoted record and
    media, so it is unwrapped one level.
    """
    kind = str(embed.get("$type") or "")
    short = kind.removeprefix(_EMBED_PREFIX) or None
    quoted = external = None
    images = 0
    media = embed
    if short == "record":
        quoted = _dict(embed.get("record")).get("uri")
    elif short == "recordWithMedia":
        quoted = _dict(_dict(embed.get("record")).get("record")).get("uri")
        media = _dict(embed.get("media"))
    media_kind = str(media.get("$type") or "").removeprefix(_EMBED_PREFIX)
    if media_kind == "external":
        external = _dict(media.get("external")).get("uri")
    elif media_kind == "images":
        images = len(_list(media.get("images")))
    return {"embed_type": short, "quoted_uri": quoted, "external_url": external, "image_count": images}


def _facet_features(record: dict[str, Any]) -> list[dict[str, Any]]:
    return [_dict(f) for facet in _list(record.get("facets")) for f in _list(_dict(facet).get("features"))]


def record_facts(record: dict[str, Any]) -> dict[str, Any]:
    """The columns a post *record* yields on its own, before any AppView hydration.

    Shared by :func:`flatten_post` (a hydrated view) and the Jetstream flattener
    (a raw record straight off the firehose), so the two read a post the same way.
    """
    reply = _dict(record.get("reply"))
    features = _facet_features(record)

    def feature(kind: str, key: str) -> list[str]:
        return _strings(f.get(key) for f in features if f.get("$type") == f"app.bsky.richtext.facet#{kind}")

    return {
        "text": record.get("text"),
        "created_at": record.get("createdAt"),
        "langs": _strings(_list(record.get("langs"))),
        "reply_parent_uri": _dict(reply.get("parent")).get("uri"),
        "reply_root_uri": _dict(reply.get("root")).get("uri"),
        **_embed_facts(_dict(record.get("embed"))),
        "hashtags": _strings([*feature("tag", "tag"), *_list(record.get("tags"))]),
        "mentions": feature("mention", "did"),
        "links": feature("link", "uri"),
    }


def flatten_post(view: dict[str, Any]) -> dict[str, Any]:
    """A post view as one row of :data:`POST_SCHEMA`."""
    author = _dict(view.get("author"))
    uri = view.get("uri")
    path = _profile_path(author.get("did"), author.get("handle"))
    rkey = rkey_of(uri)
    return {
        "uri": uri,
        "cid": view.get("cid"),
        "author_did": author.get("did"),
        "author_handle": author.get("handle"),
        "author_display_name": author.get("displayName"),
        "indexed_at": view.get("indexedAt"),
        "reply_count": view.get("replyCount"),
        "repost_count": view.get("repostCount"),
        "like_count": view.get("likeCount"),
        "quote_count": view.get("quoteCount"),
        "bookmark_count": view.get("bookmarkCount"),
        **record_facts(_dict(view.get("record"))),
        "labels": _labels(view),
        "post_url": f"{WEB_BASE}/profile/{path}/post/{rkey}" if path and rkey else None,
    }


#: A post as it appears in a feed: the post, plus why it is there.
FEED_ITEM_SCHEMA = pa.schema(
    [
        *_POST_FIELDS,
        field(
            "reason",
            pa.string(),
            "Why the post is in this feed: 'repost', 'pin', or NULL when it is simply the author's own post.",
        ),
        field("reposted_by_did", pa.string(), "DID of the account that reposted it, when reason = 'repost'."),
        field("reposted_by_handle", pa.string(), "Handle of that account."),
        field("reposted_at", TIMESTAMP, "When it was reposted, when reason = 'repost'."),
    ]
)


def flatten_feed_item(item: dict[str, Any]) -> dict[str, Any] | None:
    """A feed item as one row of :data:`FEED_ITEM_SCHEMA`, or None if it holds no post."""
    post = item.get("post")
    if not isinstance(post, dict):
        return None
    reason = _dict(item.get("reason"))
    kind = str(reason.get("$type") or "")
    short = kind.rsplit("#", 1)[-1].removeprefix("reason").lower() or None
    by = _dict(reason.get("by"))
    return {
        **flatten_post(post),
        "reason": short,
        "reposted_by_did": by.get("did"),
        "reposted_by_handle": by.get("handle"),
        "reposted_at": reason.get("indexedAt") if short == "repost" else None,
    }


#: A post within a thread, placed by its distance from the post asked about.
THREAD_SCHEMA = pa.schema(
    [
        field(
            "anchor",
            pa.string(),
            "The post reference exactly as it was passed in, so a LATERAL can join back on its input.",
        ),
        field(
            "depth",
            pa.int64(),
            "Position relative to the anchor post: 0 is the anchor, negative numbers are its ancestors "
            "(-1 the parent), positive numbers are replies (1 a direct reply).",
        ),
        *_POST_FIELDS,
    ]
)

#: A thread node whose post could not be shown — deleted, blocked, or not found.
_MISSING_NODE_TYPES = ("#notFoundPost", "#blockedPost")


def flatten_thread(anchor: str, thread: dict[str, Any]) -> list[dict[str, Any]]:
    """A ``getPostThread`` tree as rows ordered ancestors-first, then the anchor, then replies.

    Parents are a linked list upward and replies a tree downward; both are
    walked iteratively so a very deep thread cannot exhaust the stack. Deleted
    and blocked nodes carry no post and are skipped, but the walk continues
    past them.
    """

    def is_post(node: dict[str, Any]) -> bool:
        kind = str(node.get("$type") or "")
        return isinstance(node.get("post"), dict) and not kind.endswith(_MISSING_NODE_TYPES)

    ancestors: list[dict[str, Any]] = []
    node = _dict(thread.get("parent"))
    depth = -1
    while node:
        if is_post(node):
            ancestors.append({"anchor": anchor, "depth": depth, **flatten_post(node["post"])})
        node = _dict(node.get("parent"))
        depth -= 1
    rows = list(reversed(ancestors))
    if is_post(thread):
        rows.append({"anchor": anchor, "depth": 0, **flatten_post(thread["post"])})
    stack = [(_dict(reply), 1) for reply in reversed(_list(thread.get("replies")))]
    while stack:
        reply, level = stack.pop()
        if is_post(reply):
            rows.append({"anchor": anchor, "depth": level, **flatten_post(reply["post"])})
        stack.extend((_dict(r), level + 1) for r in reversed(_list(reply.get("replies"))))
    return rows


# ---------------------------------------------------------------------------
# Feeds and trends
# ---------------------------------------------------------------------------

FEED_GENERATOR_SCHEMA = pa.schema(
    [
        field(
            "uri",
            pa.string(),
            "AT-URI of the feed (at://<did>/app.bsky.feed.generator/<rkey>); the key feed() takes.",
        ),
        field("display_name", pa.string(), "The feed's name as shown in the app."),
        field("description", pa.string(), "What the feed says it shows."),
        field("creator_did", pa.string(), "DID of the account that published the feed."),
        field("creator_handle", pa.string(), "Handle of that account."),
        field("service_did", pa.string(), "DID of the service that computes the feed's contents."),
        field("like_count", pa.int64(), "Likes of the feed itself — the usual popularity measure."),
        field(
            "accepts_interactions",
            pa.bool_(),
            "Whether the feed accepts 'show more/less like this' feedback.",
        ),
        field("content_mode", pa.string(), "Declared content mode, e.g. video-only, when the feed sets one."),
        field("avatar_url", pa.string(), "CDN URL of the feed's avatar image."),
        field("indexed_at", TIMESTAMP, "When the AppView last indexed the feed's declaration."),
        field("labels", pa.list_(pa.string()), "Moderation labels applied to the feed."),
        field("feed_url", pa.string(), "Link to the feed in the Bluesky web app."),
    ]
)


def flatten_feed_generator(view: dict[str, Any]) -> dict[str, Any]:
    """A feed generator view as one row of :data:`FEED_GENERATOR_SCHEMA`."""
    creator = _dict(view.get("creator"))
    path = _profile_path(creator.get("did"), creator.get("handle"))
    rkey = rkey_of(view.get("uri"))
    mode = view.get("contentMode")
    return {
        "uri": view.get("uri"),
        "display_name": view.get("displayName"),
        "description": view.get("description"),
        "creator_did": creator.get("did"),
        "creator_handle": creator.get("handle"),
        "service_did": view.get("did"),
        "like_count": view.get("likeCount"),
        "accepts_interactions": view.get("acceptsInteractions"),
        # Lexicon tokens are fully qualified (`app.bsky.feed.defs#contentModeVideo`);
        # the fragment is the part a reader cares about.
        "content_mode": mode.rsplit("#", 1)[-1] if isinstance(mode, str) and mode else None,
        "avatar_url": view.get("avatar"),
        "indexed_at": view.get("indexedAt"),
        "labels": _labels(view),
        "feed_url": f"{WEB_BASE}/profile/{path}/feed/{rkey}" if path and rkey else None,
    }


TREND_SCHEMA = pa.schema(
    [
        field("rank", pa.int64(), "Position in Bluesky's trending list, 1 being the top trend."),
        field("topic", pa.string(), "Bluesky's opaque identifier for the trend."),
        field("display_name", pa.string(), "The trend's headline, e.g. 'Justin Verlander retires from MLB'."),
        field("description", pa.string(), "One-sentence summary of what the trend is about."),
        field("category", pa.string(), "Bluesky's category for the trend, e.g. 'sports', 'politics'."),
        field(
            "status",
            pa.string(),
            "Where the trend is in its lifecycle, as Bluesky labels it: e.g. 'trending' while growing, "
            "'saturating' near its peak, 'cooling' after it.",
        ),
        field("post_count", pa.int64(), "Count of posts Bluesky has attributed to the trend so far."),
        field("started_at", TIMESTAMP, "When Bluesky first detected the trend."),
        field(
            "actor_handles", pa.list_(pa.string()), "Handles of accounts Bluesky associates with the trend."
        ),
        field("trend_url", pa.string(), "Link to the trend's feed in the Bluesky web app."),
    ]
)


def flatten_trends(trends: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """``getTrends`` as rows of :data:`TREND_SCHEMA`, numbered in the order Bluesky ranked them."""
    rows: list[dict[str, Any]] = []
    for rank, trend in enumerate(trends, start=1):
        link = trend.get("link")
        rows.append(
            {
                "rank": rank,
                "topic": trend.get("topic"),
                "display_name": trend.get("displayName"),
                "description": trend.get("description"),
                "category": trend.get("category"),
                "status": trend.get("status"),
                "post_count": trend.get("postCount"),
                "started_at": trend.get("startedAt"),
                "actor_handles": _strings(_dict(a).get("handle") for a in _list(trend.get("actors"))),
                "trend_url": f"{WEB_BASE}{link}" if isinstance(link, str) and link.startswith("/") else None,
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Jetstream (the firehose)
# ---------------------------------------------------------------------------

JETSTREAM_SCHEMA = pa.schema(
    [
        field(
            "time_us",
            pa.int64(),
            "Jetstream's cursor: microseconds since the Unix epoch at which this Jetstream instance "
            "received the event. Strictly increasing, and the value to pass as `cursor` to resume.",
        ),
        field("event_time", TIMESTAMP, "`time_us` as a timestamp — when the event reached Jetstream."),
        field(
            "kind",
            pa.string(),
            "'commit' for a record created, updated or deleted; 'identity' for a handle or DID "
            "document change; 'account' for an account activated, deactivated or taken down.",
        ),
        field("did", pa.string(), "DID of the repository (account) the event belongs to."),
        field("operation", pa.string(), "For commits: 'create', 'update' or 'delete'. NULL otherwise."),
        field(
            "collection",
            pa.string(),
            "For commits: the record type, e.g. 'app.bsky.feed.post', 'app.bsky.feed.like', "
            "'app.bsky.graph.follow'. NULL otherwise.",
        ),
        field("rkey", pa.string(), "For commits: the record key within its collection."),
        field(
            "uri",
            pa.string(),
            "For commits: the record's AT-URI (at://<did>/<collection>/<rkey>) — for a post, the key "
            "post() and thread() take.",
        ),
        field("cid", pa.string(), "For creates and updates: content hash of the new record version."),
        field("rev", pa.string(), "For commits: the repository revision the change was committed in."),
        field(
            "record",
            pa.string(),
            "For creates and updates: the full record as JSON text. Query it with DuckDB's JSON "
            "functions, e.g. record->>'$.subject.uri'. NULL for deletes, which carry no record.",
        ),
        field("text", pa.string(), "For a post: its text."),
        field(
            "created_at",
            TIMESTAMP,
            "The record's own createdAt, as the author's client wrote it; can be backdated.",
        ),
        field("langs", pa.list_(pa.string()), "For a post: the language tags its client declared."),
        field("reply_parent_uri", pa.string(), "For a reply: AT-URI of the post it replies to."),
        field("reply_root_uri", pa.string(), "For a reply: AT-URI of the thread's first post."),
        field(
            "embed_type",
            pa.string(),
            "For a post: what is attached — 'images', 'video', 'external', 'record', "
            "'recordWithMedia' — or NULL.",
        ),
        field("quoted_uri", pa.string(), "For a quote post: AT-URI of the quoted record."),
        field("external_url", pa.string(), "For a post with a link card: its URL."),
        field("image_count", pa.int64(), "For a post: number of attached images (0 when none)."),
        field("hashtags", pa.list_(pa.string()), "For a post: its hashtags, without '#'."),
        field("mentions", pa.list_(pa.string()), "For a post: DIDs mentioned in its text."),
        field("links", pa.list_(pa.string()), "For a post: URLs linked from its text."),
        field(
            "subject_uri",
            pa.string(),
            "For a like or repost: AT-URI of the post (or feed) acted on.",
        ),
        field(
            "subject_did",
            pa.string(),
            "The account acted on: the followed/blocked DID for a follow or block, the author of the "
            "liked or reposted record for a like or repost.",
        ),
        field("handle", pa.string(), "For an identity event: the handle, when the event carries one."),
        field("active", pa.bool_(), "For an account event: whether the account is now active."),
        field(
            "account_status",
            pa.string(),
            "For an inactive account: why — e.g. 'deactivated', 'takendown', 'suspended', 'deleted'.",
        ),
    ]
)

#: Record types whose payload is a post, for which the post columns are filled.
_POST_COLLECTION = "app.bsky.feed.post"


def _did_of_uri(uri: Any) -> str | None:
    """The authority of an AT-URI when it is a DID."""
    if not isinstance(uri, str) or not uri.startswith("at://did:"):
        return None
    return uri[len("at://") :].split("/", 1)[0]


def flatten_jetstream_event(event: dict[str, Any]) -> dict[str, Any]:
    """One Jetstream JSON event as a row of :data:`JETSTREAM_SCHEMA`.

    Total like everything here: a malformed event yields a row of NULLs around
    whatever did parse, never an exception.
    """
    did = event.get("did")
    time_us = to_integer(event.get("time_us"))
    row: dict[str, Any] = dict.fromkeys(JETSTREAM_SCHEMA.names)
    row |= {
        "time_us": time_us,
        "event_time": datetime.fromtimestamp(time_us / 1_000_000, tz=UTC) if time_us is not None else None,
        "kind": event.get("kind"),
        "did": did,
    }
    commit = _dict(event.get("commit"))
    if commit:
        collection, rkey = commit.get("collection"), commit.get("rkey")
        record = commit.get("record")
        row |= {
            "operation": commit.get("operation"),
            "collection": collection,
            "rkey": rkey,
            "uri": f"at://{did}/{collection}/{rkey}" if did and collection and rkey else None,
            "cid": commit.get("cid"),
            "rev": commit.get("rev"),
        }
        if isinstance(record, dict):
            row["record"] = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
            row["created_at"] = record.get("createdAt")
            if collection == _POST_COLLECTION:
                row |= record_facts(record)
            subject = record.get("subject")
            if isinstance(subject, dict):
                row["subject_uri"] = subject.get("uri")
                row["subject_did"] = _did_of_uri(subject.get("uri"))
            elif isinstance(subject, str) and subject.startswith("did:"):
                row["subject_did"] = subject
    identity = _dict(event.get("identity"))
    if identity:
        row["handle"] = identity.get("handle")
    account = _dict(event.get("account"))
    if account:
        row["active"] = account.get("active")
        row["account_status"] = account.get("status")
    return row
