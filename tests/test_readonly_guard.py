"""Structural guard: this worker must never write to Bluesky.

Every write in the AT Protocol — posting, liking, following, deleting, even
logging in — is an XRPC *procedure*, sent as ``POST``. The worker is read-only
by construction: a single ``_get`` chokepoint that issues ``GET`` and refuses
any method outside an allow-list of lexicon *queries*. These tests fail the
build if that ever stops being true.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import httpx
import pytest

from vgi_bluesky import bluesky_api

PACKAGE = Path(__file__).resolve().parent.parent / "vgi_bluesky"

#: httpx verbs that could mutate state on the server.
WRITE_VERBS = {"post", "put", "patch", "delete", "request", "stream", "send"}

#: Procedure names (and name fragments) that write to a repository or account.
#: None may appear anywhere in the package source.
PROCEDURES = (
    "createRecord",
    "putRecord",
    "deleteRecord",
    "applyWrites",
    "uploadBlob",
    "createSession",
    "refreshSession",
    "createAccount",
    "deleteAccount",
    "muteActor",
    "putPreferences",
    "updateSeen",
    "sendMessage",
)


def _calls(tree: ast.AST) -> list[ast.Call]:
    return [node for node in ast.walk(tree) if isinstance(node, ast.Call)]


class TestNoWriteVerbs:
    def test_no_http_write_calls_anywhere(self) -> None:
        offenders: list[str] = []
        for path in PACKAGE.rglob("*.py"):
            tree = ast.parse(path.read_text())
            for call in _calls(tree):
                if isinstance(call.func, ast.Attribute) and call.func.attr in WRITE_VERBS:
                    offenders.append(f"{path.name}: .{call.func.attr}()")
        assert offenders == [], f"write-shaped HTTP calls found: {offenders}"

    def test_single_http_chokepoint(self) -> None:
        """Only bluesky_api.py may reference httpx at all."""
        users = sorted(path.name for path in PACKAGE.rglob("*.py") if "httpx" in path.read_text())
        assert users == ["bluesky_api.py"], users

    def test_single_websocket_chokepoint(self) -> None:
        """Only jetstream.py may open a WebSocket, and it never sends on one.

        `.send()` is already in WRITE_VERBS, so the AST check above covers the
        second half; this pins the first.
        """
        users = sorted(path.name for path in PACKAGE.rglob("*.py") if "websockets" in path.read_text())
        assert users == ["jetstream.py"], users

    @pytest.mark.parametrize("procedure", PROCEDURES)
    def test_no_procedure_names_in_source(self, procedure: str) -> None:
        for path in PACKAGE.rglob("*.py"):
            assert procedure not in path.read_text(), f"{path.name} references {procedure}"


class TestMethodAllowList:
    """The chokepoint refuses anything but the lexicon queries it was built for."""

    def test_every_allowed_method_is_a_query_name(self) -> None:
        """Lexicon queries are named get*/search*/resolve*; procedures are verbs like create*."""
        for method in bluesky_api.READ_METHODS:
            name = method.rsplit(".", 1)[-1]
            assert re.match(r"^(get|search|resolve)[A-Z]", name), method

    def test_every_method_the_package_calls_is_allowed(self) -> None:
        """An NSID used anywhere in the package must be on the allow-list, or it would fail at runtime."""
        nsid = re.compile(r'"((?:app\.bsky|com\.atproto)\.[a-z]+\.[a-zA-Z]+)"')
        used = {m for path in PACKAGE.rglob("*.py") for m in nsid.findall(path.read_text())}
        # Record collection names also match the NSID shape; they are not methods.
        used -= {"app.bsky.feed.post", "app.bsky.feed.generator"}
        assert used <= bluesky_api.READ_METHODS, sorted(used - bluesky_api.READ_METHODS)

    def test_an_unlisted_method_is_refused_before_any_request(self) -> None:
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(str(request.url))
            return httpx.Response(200, json={})

        client = httpx.Client(transport=httpx.MockTransport(handler))
        with pytest.raises(ValueError, match="not a read-only XRPC query"):
            bluesky_api._get("com.atproto.repo.createRecord", client=client)
        assert seen == []


class TestNoInjection:
    """A value from user SQL must stay a query-string value.

    XRPC puts every argument in the query string, so there is no path segment
    to traverse out of. What remains is that the value is encoded, not spliced:
    an ``&`` in an actor must not add a parameter.
    """

    def test_an_ampersand_cannot_add_a_parameter(self) -> None:
        seen: list[httpx.URL] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.url)
            return httpx.Response(200, json={"profiles": []})

        client = httpx.Client(transport=httpx.MockTransport(handler))
        bluesky_api.profiles(["alice.test&limit=9999"], client=client)
        assert seen[0].params.get_list("actors") == ["alice.test&limit=9999"]
        assert "limit" not in seen[0].params

    def test_the_path_is_always_the_fixed_method(self) -> None:
        seen: list[httpx.URL] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.url)
            return httpx.Response(200, json={"posts": []})

        client = httpx.Client(transport=httpx.MockTransport(handler))
        bluesky_api.posts(
            ["at://did:plc:x/app.bsky.feed.post/../../com.atproto.repo.createRecord"], client=client
        )
        assert seen[0].path == "/xrpc/app.bsky.feed.getPosts"
