"""Flattening Bluesky's nested views into rows, and the totality of every conversion."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
import pytest

from vgi_bluesky import schemas
from vgi_bluesky.meta import comment_of
from vgi_bluesky.schemas import (
    ACTOR_SCHEMA,
    FEED_GENERATOR_SCHEMA,
    FEED_ITEM_SCHEMA,
    GRAPH_SCHEMA,
    INTERACTION_SCHEMA,
    POST_SCHEMA,
    PROFILE_SCHEMA,
    THREAD_SCHEMA,
    TREND_SCHEMA,
    batch_from_rows,
    flatten_feed_generator,
    flatten_feed_item,
    flatten_post,
    flatten_profile,
    flatten_thread,
    flatten_trends,
    to_timestamp,
)

ALL_SCHEMAS = [
    ACTOR_SCHEMA,
    PROFILE_SCHEMA,
    GRAPH_SCHEMA,
    INTERACTION_SCHEMA,
    POST_SCHEMA,
    FEED_ITEM_SCHEMA,
    THREAD_SCHEMA,
    FEED_GENERATOR_SCHEMA,
    TREND_SCHEMA,
]

BSKY = "did:plc:z72i7hdynmk6r22z27h6tvur"


def _post(rkey: str = "3mw2cdr44fc2a", *, reply_to: str | None = None, **record: Any) -> dict[str, Any]:
    """A post view shaped like the AppView's, trimmed to what the flattener reads."""
    body: dict[str, Any] = {
        "$type": "app.bsky.feed.post",
        "createdAt": "2026-09-21T18:04:06.128Z",
        "text": "If you're in line to vote for @bsky38.com, please stay in line!",
        "langs": ["en"],
        **record,
    }
    if reply_to:
        body["reply"] = {"parent": {"uri": reply_to}, "root": {"uri": reply_to}}
    return {
        "uri": f"at://{BSKY}/app.bsky.feed.post/{rkey}",
        "cid": "bafyrei",
        "author": {"did": BSKY, "handle": "bsky.app", "displayName": "Bluesky"},
        "record": body,
        "replyCount": 87,
        "repostCount": 85,
        "likeCount": 717,
        "quoteCount": 45,
        "bookmarkCount": 39,
        "indexedAt": "2026-09-21T18:04:06.866Z",
        "labels": [{"val": "!no-unauthenticated"}],
    }


class TestSchemasAreDocumented:
    @pytest.mark.parametrize("schema", ALL_SCHEMAS)
    def test_every_column_has_a_comment(self, schema: pa.Schema) -> None:
        missing = [f.name for f in schema if not comment_of(f)]
        assert missing == []

    @pytest.mark.parametrize("schema", ALL_SCHEMAS)
    def test_column_names_are_unique(self, schema: pa.Schema) -> None:
        assert len(schema.names) == len(set(schema.names))

    @pytest.mark.parametrize("schema", ALL_SCHEMAS)
    def test_an_empty_batch_builds(self, schema: pa.Schema) -> None:
        assert batch_from_rows([], schema).num_rows == 0


class TestPost:
    def test_core_fields(self) -> None:
        row = flatten_post(_post())
        assert row["author_handle"] == "bsky.app"
        assert row["like_count"] == 717
        assert row["langs"] == ["en"]
        assert row["labels"] == ["!no-unauthenticated"]
        assert row["post_url"] == "https://bsky.app/profile/bsky.app/post/3mw2cdr44fc2a"
        assert row["reply_parent_uri"] is None

    def test_facets_become_lists(self) -> None:
        facets = [
            {"features": [{"$type": "app.bsky.richtext.facet#mention", "did": "did:web:bsky38.com"}]},
            {"features": [{"$type": "app.bsky.richtext.facet#link", "uri": "https://bsky38.com/"}]},
            {"features": [{"$type": "app.bsky.richtext.facet#tag", "tag": "duckdb"}]},
        ]
        row = flatten_post(_post(facets=facets, tags=["sql", "duckdb"]))
        assert row["mentions"] == ["did:web:bsky38.com"]
        assert row["links"] == ["https://bsky38.com/"]
        assert row["hashtags"] == ["duckdb", "sql"]

    @pytest.mark.parametrize(
        ("embed", "expected"),
        [
            (
                {"$type": "app.bsky.embed.record", "record": {"uri": "at://q/app.bsky.feed.post/1"}},
                {"embed_type": "record", "quoted_uri": "at://q/app.bsky.feed.post/1", "image_count": 0},
            ),
            (
                {"$type": "app.bsky.embed.images", "images": [{}, {}, {}]},
                {"embed_type": "images", "quoted_uri": None, "image_count": 3},
            ),
            (
                {"$type": "app.bsky.embed.external", "external": {"uri": "https://duckdb.org"}},
                {"embed_type": "external", "external_url": "https://duckdb.org"},
            ),
            (
                {
                    "$type": "app.bsky.embed.recordWithMedia",
                    "record": {"record": {"uri": "at://q/app.bsky.feed.post/2"}},
                    "media": {"$type": "app.bsky.embed.images", "images": [{}]},
                },
                {
                    "embed_type": "recordWithMedia",
                    "quoted_uri": "at://q/app.bsky.feed.post/2",
                    "image_count": 1,
                },
            ),
        ],
    )
    def test_embeds(self, embed: dict[str, Any], expected: dict[str, Any]) -> None:
        row = flatten_post(_post(embed=embed))
        assert {k: row[k] for k in expected} == expected

    def test_invalid_handle_links_by_did(self) -> None:
        view = _post()
        view["author"]["handle"] = "handle.invalid"
        assert flatten_post(view)["post_url"] == f"https://bsky.app/profile/{BSKY}/post/3mw2cdr44fc2a"

    def test_garbage_record_does_not_raise(self) -> None:
        view = {"uri": "at://x/app.bsky.feed.post/1", "record": "not a dict", "author": None, "labels": "x"}
        row = flatten_post(view)
        batch = batch_from_rows([row], POST_SCHEMA)
        assert batch.num_rows == 1


class TestFeedItem:
    def test_repost_reason(self) -> None:
        item = {
            "post": _post(),
            "reason": {
                "$type": "app.bsky.feed.defs#reasonRepost",
                "by": {"did": "did:plc:r", "handle": "reposter.test"},
                "indexedAt": "2026-09-22T00:00:00Z",
            },
        }
        row = flatten_feed_item(item)
        assert row is not None
        assert row["reason"] == "repost"
        assert row["reposted_by_handle"] == "reposter.test"
        assert row["reposted_at"] == "2026-09-22T00:00:00Z"

    def test_pin_reason_has_no_repost_time(self) -> None:
        row = flatten_feed_item({"post": _post(), "reason": {"$type": "app.bsky.feed.defs#reasonPin"}})
        assert row is not None and row["reason"] == "pin" and row["reposted_at"] is None

    def test_own_post_has_no_reason(self) -> None:
        row = flatten_feed_item({"post": _post()})
        assert row is not None and row["reason"] is None

    def test_item_without_a_post_is_dropped(self) -> None:
        assert flatten_feed_item({"reason": {}}) is None


class TestThread:
    def test_order_and_depth(self) -> None:
        root = _post("root")
        parent = _post("parent", reply_to=root["uri"])
        anchor = _post("anchor", reply_to=parent["uri"])
        reply = _post("reply", reply_to=anchor["uri"])
        nested = _post("nested", reply_to=reply["uri"])
        sibling = _post("sibling", reply_to=anchor["uri"])
        thread = {
            "$type": "app.bsky.feed.defs#threadViewPost",
            "post": anchor,
            "parent": {
                "$type": "app.bsky.feed.defs#threadViewPost",
                "post": parent,
                "parent": {"$type": "app.bsky.feed.defs#threadViewPost", "post": root},
            },
            "replies": [
                {
                    "$type": "app.bsky.feed.defs#threadViewPost",
                    "post": reply,
                    "replies": [{"$type": "app.bsky.feed.defs#threadViewPost", "post": nested}],
                },
                {"$type": "app.bsky.feed.defs#notFoundPost", "uri": "at://gone", "notFound": True},
                {"$type": "app.bsky.feed.defs#threadViewPost", "post": sibling},
            ],
        }
        rows = flatten_thread("input", thread)
        assert [(r["depth"], r["uri"].rsplit("/", 1)[-1]) for r in rows] == [
            (-2, "root"),
            (-1, "parent"),
            (0, "anchor"),
            (1, "reply"),
            (2, "nested"),
            (1, "sibling"),
        ]
        assert {r["anchor"] for r in rows} == {"input"}

    def test_a_blocked_parent_is_skipped_but_the_walk_continues(self) -> None:
        root = _post("root")
        thread = {
            "post": _post("anchor"),
            "parent": {
                "$type": "app.bsky.feed.defs#blockedPost",
                "uri": "at://blocked",
                "parent": {"$type": "app.bsky.feed.defs#threadViewPost", "post": root},
            },
        }
        rows = flatten_thread("x", thread)
        assert [(r["depth"], r["uri"].rsplit("/", 1)[-1]) for r in rows] == [(-2, "root"), (0, "anchor")]

    def test_a_very_deep_thread_does_not_recurse(self) -> None:
        node: dict[str, Any] = {"post": _post("leaf")}
        for i in range(5000):
            node = {"post": _post(f"n{i}"), "replies": [node]}
        assert len(flatten_thread("x", node)) == 5001


class TestProfileAndFeeds:
    def test_profile(self) -> None:
        view = {
            "did": BSKY,
            "handle": "bsky.app",
            "followersCount": 35_065_784,
            "associated": {"lists": 18, "feedgens": 7, "starterPacks": 15, "labeler": False},
            "verification": {"verifiedStatus": "none"},
        }
        row = flatten_profile("@BSKY.APP", view)
        assert row["actor"] == "@BSKY.APP"
        assert (row["feeds_count"], row["is_labeler"], row["verified_status"]) == (7, False, "none")
        assert batch_from_rows([row], PROFILE_SCHEMA).column("followers_count").to_pylist() == [35_065_784]

    def test_feed_generator(self) -> None:
        row = flatten_feed_generator(
            {
                "uri": f"at://{BSKY}/app.bsky.feed.generator/whats-hot",
                "did": "did:web:discover.bsky.app",
                "creator": {"did": BSKY, "handle": "bsky.app"},
                "contentMode": "app.bsky.feed.defs#contentModeVideo",
            }
        )
        assert row["content_mode"] == "contentModeVideo"
        assert row["feed_url"] == "https://bsky.app/profile/bsky.app/feed/whats-hot"
        assert row["service_did"] == "did:web:discover.bsky.app"

    def test_trends_are_ranked_in_order(self) -> None:
        rows = flatten_trends(
            [
                {"displayName": "A", "link": "/profile/did:plc:x/feed/1", "actors": [{"handle": "a.test"}]},
                {"displayName": "B", "link": "not a path"},
            ]
        )
        assert [(r["rank"], r["display_name"]) for r in rows] == [(1, "A"), (2, "B")]
        assert rows[0]["trend_url"] == "https://bsky.app/profile/did:plc:x/feed/1"
        assert rows[0]["actor_handles"] == ["a.test"]
        assert rows[1]["trend_url"] is None


class TestTimestamps:
    def test_zulu_and_offset_forms(self) -> None:
        assert to_timestamp("2026-09-21T18:04:06.128Z") == datetime(2026, 9, 21, 18, 4, 6, 128000, tzinfo=UTC)
        assert to_timestamp("2026-09-26T15:34:19.859977+00:00") is not None

    def test_a_naive_timestamp_is_read_as_utc(self) -> None:
        assert to_timestamp("2026-01-01T00:00:00") == datetime(2026, 1, 1, tzinfo=UTC)

    @pytest.mark.parametrize(
        "value", ["0001-01-01T00:00:00Z", "9999-12-31T23:59:59Z", "yesterday", "", None, 12345, True]
    )
    def test_unrepresentable_values_become_null(self, value: Any) -> None:
        """Client-authored createdAt values can be anything; none may fail a batch."""
        assert to_timestamp(value) is None

    def test_one_bad_value_does_not_cost_the_batch(self) -> None:
        rows = [{"created_at": "2026-01-01T00:00:00Z"}, {"created_at": "0001-01-01T00:00:00Z"}]
        schema = pa.schema([schemas.field("created_at", schemas.TIMESTAMP, "t")])
        assert batch_from_rows(rows, schema).column(0).null_count == 1


class TestTotality:
    def test_a_non_integer_count_becomes_null(self) -> None:
        row = flatten_post(_post())
        row["like_count"] = "lots"
        assert batch_from_rows([row], POST_SCHEMA).column("like_count").to_pylist() == [None]

    def test_a_malformed_list_becomes_null(self) -> None:
        row = flatten_post(_post())
        row["langs"] = [{"nested": "dict"}]
        assert batch_from_rows([row], POST_SCHEMA).column("langs").to_pylist() == [None]
