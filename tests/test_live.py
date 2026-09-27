"""Facts about the live Bluesky API that this worker's design depends on.

Each test pins down one behaviour that was found by probing the API and that
some code path relies on. If Bluesky changes one of them, the matching test is
what says which assumption broke. Opt in with ``pytest -m live``.
"""

from __future__ import annotations

import httpx
import pytest

from vgi_bluesky import bluesky_api as api
from vgi_bluesky.bluesky_api import BlueskyError

pytestmark = pytest.mark.live

BSKY_DID = "did:plc:z72i7hdynmk6r22z27h6tvur"


def _latest_post_uri() -> str:
    _payload, rows, _cursor = api.page(
        "app.bsky.feed.getAuthorFeed",
        "feed",
        {"actor": "bsky.app", "filter": "posts_no_replies"},
        page_limit=10,
    )
    return next(row["post"]["uri"] for row in rows if "reason" not in row)


class TestLimits:
    def test_page_limit_is_100(self) -> None:
        with pytest.raises(BlueskyError) as info:
            api.page("app.bsky.graph.getFollowers", "followers", {"actor": "bsky.app"}, page_limit=101)
        assert info.value.status == 400

    def test_batch_limit_is_25(self) -> None:
        with pytest.raises(BlueskyError) as info:
            api._get("app.bsky.actor.getProfiles", [("actors", "bsky.app")] * 26)
        assert info.value.status == 400

    def test_trends_limit_is_25(self) -> None:
        with pytest.raises(BlueskyError):
            api._get("app.bsky.unspecced.getTrends", {"limit": 26})


class TestFreshness:
    def test_the_public_appview_declares_max_age(self) -> None:
        hint = api.CacheHint()
        api.trends(hint=hint)
        assert hint.cacheable and hint.max_age is not None


class TestIdentifiers:
    def test_get_posts_rejects_a_handle_authority(self) -> None:
        """Why canonical_uri resolves handles: getPosts answers 500 rather than resolving."""
        uri = _latest_post_uri().replace(BSKY_DID, "bsky.app")
        with pytest.raises(BlueskyError) as info:
            api._get("app.bsky.feed.getPosts", {"uris": uri})
        assert info.value.status >= 500

    def test_canonical_uri_makes_it_work(self) -> None:
        uri = _latest_post_uri()
        web = uri.replace(f"at://{BSKY_DID}/app.bsky.feed.post/", "https://bsky.app/profile/bsky.app/post/")
        canonical = api.canonical_uri(web, dids={})
        assert canonical == uri
        assert [p["uri"] for p in api.posts([canonical])] == [uri]

    def test_get_profiles_omits_unknown_actors(self) -> None:
        found = api.profiles(["bsky.app", "no-such-user-xyz-000.bsky.social"])
        assert [p["handle"] for p in found] == ["bsky.app"]


class TestSearch:
    def test_search_is_refused_on_the_public_appview(self) -> None:
        """Why search goes to a different host."""
        response = httpx.get(
            f"{api.DEFAULT_APPVIEW_URL}/xrpc/app.bsky.feed.searchPosts", params={"q": "duckdb", "limit": 1}
        )
        assert response.status_code == 403

    def test_the_first_page_is_served_anonymously(self) -> None:
        _payload, rows, cursor = api.page(
            "app.bsky.feed.searchPosts", "posts", {"q": "bluesky"}, base=api.search_url()
        )
        assert rows and cursor

    def test_the_cursor_is_refused_anonymously(self) -> None:
        """Why search_posts stops after one page."""
        _payload, _rows, cursor = api.page(
            "app.bsky.feed.searchPosts", "posts", {"q": "bluesky"}, base=api.search_url()
        )
        with pytest.raises(BlueskyError) as info:
            api.page(
                "app.bsky.feed.searchPosts", "posts", {"q": "bluesky"}, cursor=cursor, base=api.search_url()
            )
        assert info.value.status == 403


class TestJetstream:
    """The Jetstream behaviours jetstream() is built on."""

    @staticmethod
    def _read(url: str, n: int, seconds: float = 8.0) -> list[dict]:
        import contextlib
        import json
        import time

        from websockets.sync.client import connect

        events: list[dict] = []
        deadline = time.time() + seconds
        with connect(url, open_timeout=10, close_timeout=0.5, max_size=None) as socket:
            while len(events) < n and time.time() < deadline:
                with contextlib.suppress(TimeoutError):
                    events.append(json.loads(socket.recv(timeout=1)))
        return events

    def test_time_us_is_strictly_increasing(self) -> None:
        """Why an upper bound on event_time is a safe stopping rule."""
        from vgi_bluesky.jetstream import JETSTREAM_HOSTS, subscribe_url

        events = self._read(subscribe_url(JETSTREAM_HOSTS[0], collections=[], dids=[], cursor=0), 500)
        times = [e["time_us"] for e in events]
        assert len(times) > 100
        assert all(a < b for a, b in zip(times, times[1:], strict=False))

    def test_a_collection_filter_is_a_superset(self) -> None:
        """Only commits are filtered; nothing of the wanted collection is withheld."""
        from vgi_bluesky.jetstream import JETSTREAM_HOSTS, subscribe_url

        url = subscribe_url(JETSTREAM_HOSTS[0], collections=["app.bsky.graph.follow"], dids=[], cursor=0)
        commits = [e for e in self._read(url, 200) if e.get("kind") == "commit"]
        assert commits and {e["commit"]["collection"] for e in commits} == {"app.bsky.graph.follow"}

    def test_replay_starts_at_the_cursor(self) -> None:
        import time

        from vgi_bluesky.jetstream import JETSTREAM_HOSTS, subscribe_url

        cursor = int((time.time() - 600) * 1_000_000)
        events = self._read(subscribe_url(JETSTREAM_HOSTS[0], collections=[], dids=[], cursor=cursor), 5)
        assert events and cursor <= events[0]["time_us"] < cursor + 5_000_000
