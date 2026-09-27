"""Shared fixtures and hooks for the test suite.

Two things live here: a hermetic connection pool for every test, and rate-limit
handling for the live tests. Bluesky limits anonymous traffic per IP, so a live
test can fail because something *else* on the same IP was talking to Bluesky at
the same time. That failure says nothing about the code, so a 429 that survives
``bluesky_api._get``'s own retries becomes a skip with a loud reason.
"""

from __future__ import annotations

from collections.abc import Generator
from typing import Any

import pytest

from vgi_bluesky import bluesky_api, jetstream
from vgi_bluesky.bluesky_api import BlueskyError


@pytest.fixture(autouse=True)
def _hermetic_connection_pool() -> Generator[None]:
    """Give every test a clean connection pool.

    `bluesky_api` keeps a process-wide client so a paged scan does not pay a TLS
    handshake per page. Without resetting it, a client built against one test's
    mock transport would serve the next.
    """
    bluesky_api.reset_shared_client()
    yield
    bluesky_api.reset_shared_client()
    jetstream.close_all()


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[Any]) -> Generator[None, Any]:
    """Rewrite an exhausted-rate-limit failure into a skip, for live tests only.

    Scoped to the ``live`` marker on purpose: an offline test seeing a 429 means
    a mocked transport produced one, which is a real failure of that test.
    """
    outcome = yield
    report = outcome.get_result()
    if item.get_closest_marker("live") is None or report.outcome != "failed":
        return
    exception = getattr(call, "excinfo", None)
    if exception is None or not isinstance(exception.value, BlueskyError):
        return
    if exception.value.status != 429:
        return
    report.outcome = "skipped"
    report.longrepr = (
        str(item.path),
        item.location[1] or 0,
        "Skipped: Bluesky rate limit exhausted after retries — another client is "
        "sharing this IP's budget. Re-run when it is quiet.",
    )
