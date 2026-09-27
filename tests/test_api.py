"""The HTTP layer, against a mocked transport: identifiers, batching, retries, freshness."""

from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from vgi_bluesky import bluesky_api as api
from vgi_bluesky.bluesky_api import BlueskyError, CacheHint

Handler = Callable[[httpx.Request], httpx.Response]


def _client(handler: Handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


class TestNormalizeActor:
    @pytest.mark.parametrize(
        ("given", "expected"),
        [
            ("bsky.app", "bsky.app"),
            ("@bsky.app", "bsky.app"),
            ("  BSKY.App ", "bsky.app"),
            ("https://bsky.app/profile/jay.bsky.team", "jay.bsky.team"),
            ("https://bsky.app/profile/jay.bsky.team/post/3abc", "jay.bsky.team"),
            ("did:plc:Z72I7hdynmk6r22z27h6tvur", "did:plc:Z72I7hdynmk6r22z27h6tvur"),
        ],
    )
    def test_forms_people_paste(self, given: str, expected: str) -> None:
        assert api.normalize_actor(given) == expected

    def test_a_did_keeps_its_case(self) -> None:
        """Handles are case-insensitive; DIDs are not, so only handles are folded."""
        assert api.normalize_actor("did:web:Example.COM") == "did:web:Example.COM"


class TestPostUriParts:
    @pytest.mark.parametrize(
        ("given", "expected"),
        [
            (
                "at://did:plc:abc/app.bsky.feed.post/3k2",
                ("did:plc:abc", "app.bsky.feed.post", "3k2"),
            ),
            (
                "https://bsky.app/profile/Bsky.App/post/3k2",
                ("bsky.app", "app.bsky.feed.post", "3k2"),
            ),
            (
                "https://bsky.app/profile/bsky.app/feed/whats-hot",
                ("bsky.app", "app.bsky.feed.generator", "whats-hot"),
            ),
        ],
    )
    def test_accepted_forms(self, given: str, expected: tuple[str, str, str]) -> None:
        assert api.post_uri_parts(given) == expected

    @pytest.mark.parametrize(
        "given",
        ["garbage", "", "at://did:plc:abc", "https://bsky.app/profile/x", "https://example.com/a/b/c/d"],
    )
    def test_rejected_forms(self, given: str) -> None:
        assert api.post_uri_parts(given) is None


class TestCanonicalUri:
    """getPosts answers 500 to a handle-authority URI, so handles are resolved first."""

    def test_a_did_uri_needs_no_request(self) -> None:
        client = _client(lambda request: pytest.fail(f"unexpected request {request.url}"))
        uri = api.canonical_uri("at://did:plc:abc/app.bsky.feed.post/1", client=client, dids={})
        assert uri == "at://did:plc:abc/app.bsky.feed.post/1"

    def test_a_handle_is_resolved_once_per_call(self) -> None:
        calls: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.url.params["handle"])
            return httpx.Response(200, json={"did": "did:plc:resolved"})

        client = _client(handler)
        dids: dict[str, str | None] = {}
        first = api.canonical_uri("https://bsky.app/profile/alice.test/post/1", client=client, dids=dids)
        second = api.canonical_uri("at://alice.test/app.bsky.feed.post/2", client=client, dids=dids)
        assert first == "at://did:plc:resolved/app.bsky.feed.post/1"
        assert second == "at://did:plc:resolved/app.bsky.feed.post/2"
        assert calls == ["alice.test"]

    def test_an_unresolvable_handle_yields_none(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                400, json={"error": "InvalidRequest", "message": "Unable to resolve handle"}
            )

        uri = api.canonical_uri("at://nobody.test/app.bsky.feed.post/1", client=_client(handler), dids={})
        assert uri is None


class TestBatching:
    def test_profiles_chunk_at_25_and_repeat_the_parameter(self) -> None:
        chunks: list[list[str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            chunks.append(request.url.params.get_list("actors"))
            return httpx.Response(200, json={"profiles": []})

        api.profiles([f"a{i}.test" for i in range(60)], client=_client(handler))
        assert [len(c) for c in chunks] == [25, 25, 10]

    def test_duplicates_are_fetched_once(self) -> None:
        chunks: list[list[str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            chunks.append(request.url.params.get_list("uris"))
            return httpx.Response(200, json={"posts": []})

        api.posts(["at://a/p/1", "at://a/p/1", "at://a/p/2"], client=_client(handler))
        assert chunks == [["at://a/p/1", "at://a/p/2"]]


class TestPage:
    def test_limit_and_cursor_are_sent(self) -> None:
        seen: list[httpx.QueryParams] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.url.params)
            return httpx.Response(200, json={"followers": [{"did": "x"}], "cursor": "next"})

        payload, rows, cursor = api.page(
            "app.bsky.graph.getFollowers", "followers", {"actor": "a"}, cursor="c1", client=_client(handler)
        )
        assert seen[0]["limit"] == "100" and seen[0]["cursor"] == "c1"
        assert rows == [{"did": "x"}] and cursor == "next"

    def test_an_empty_page_ends_the_walk_even_with_a_cursor(self) -> None:
        """getLists has been seen returning [] with a cursor; following it would loop."""
        client = _client(lambda _r: httpx.Response(200, json={"lists": [], "cursor": "more"}))
        _payload, rows, cursor = api.page("app.bsky.feed.getActorFeeds", "lists", {}, client=client)
        assert rows == [] and cursor is None

    def test_non_dict_rows_are_dropped(self) -> None:
        client = _client(lambda _r: httpx.Response(200, json={"feeds": [{"uri": "a"}, None, "x"]}))
        _payload, rows, _cursor = api.page("app.bsky.feed.getActorFeeds", "feeds", {}, client=client)
        assert rows == [{"uri": "a"}]


class TestRetries:
    @pytest.fixture(autouse=True)
    def _no_sleep(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(api.time, "sleep", lambda _s: None)

    def test_a_429_is_retried(self) -> None:
        statuses = iter([429, 429, 200])

        def handler(_request: httpx.Request) -> httpx.Response:
            status = next(statuses)
            return httpx.Response(status, json={"did": "did:plc:ok"} if status == 200 else {})

        assert api.resolve_handle("a.test", client=_client(handler)) == "did:plc:ok"

    def test_retry_after_is_honoured_but_capped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        slept: list[float] = []
        monkeypatch.setattr(api.time, "sleep", slept.append)
        statuses = iter([429, 200])

        def handler(_request: httpx.Request) -> httpx.Response:
            status = next(statuses)
            headers = {"retry-after": "9999"} if status == 429 else {}
            return httpx.Response(status, json={"did": "d"}, headers=headers)

        api.resolve_handle("a.test", client=_client(handler))
        assert slept == [30.0]

    def test_a_400_is_not_retried(self) -> None:
        calls: list[int] = []

        def handler(_request: httpx.Request) -> httpx.Response:
            calls.append(1)
            return httpx.Response(400, json={"error": "InvalidRequest"})

        with pytest.raises(BlueskyError) as info:
            api.trends(client=_client(handler))
        assert info.value.status == 400 and len(calls) == 1

    def test_a_non_json_200_is_a_named_error(self) -> None:
        client = _client(lambda _r: httpx.Response(200, text="<html>oops</html>"))
        with pytest.raises(BlueskyError, match="expected JSON"):
            api.trends(client=client)


class TestCacheHint:
    @pytest.mark.parametrize(
        ("header", "cacheable", "max_age"),
        [
            ("public, max-age=30", True, 30),
            ("no-cache, no-store, max-age=0", False, None),
            ("public, max-age=0", False, None),
            ("private, max-age=60", False, None),
            ("", False, None),
        ],
    )
    def test_directives(self, header: str, cacheable: bool, max_age: int | None) -> None:
        hint = CacheHint()
        hint.observe(httpx.Response(200, headers={"cache-control": header}))
        assert hint.cacheable is cacheable
        assert hint.max_age == max_age

    def test_the_shortest_lifetime_wins(self) -> None:
        hint = CacheHint()
        for age in (30, 5, 60):
            hint.observe(httpx.Response(200, headers={"cache-control": f"public, max-age={age}"}))
        assert hint.max_age == 5

    def test_an_error_response_does_not_set_freshness(self) -> None:
        """A 400 carries the CDN's error policy (max-age=5), not the resource's."""
        client = _client(
            lambda _r: httpx.Response(400, json={}, headers={"cache-control": "public, max-age=5"})
        )
        hint = CacheHint()
        with pytest.raises(BlueskyError):
            api.trends(client=client, hint=hint)
        assert hint.max_age is None


class TestHosts:
    def test_search_goes_to_the_search_host(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("BLUESKY_SEARCH_URL", raising=False)
        assert api.search_url() == "https://api.bsky.app"

    def test_hosts_are_overridable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BLUESKY_APPVIEW_URL", "https://appview.example/")
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(str(request.url))
            return httpx.Response(200, json={"trends": []})

        api.trends(client=_client(handler))
        assert seen[0].startswith("https://appview.example/xrpc/app.bsky.unspecced.getTrends")
