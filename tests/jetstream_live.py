"""Reading Jetstream directly for the live tests, failing over between instances.

A CI runner sometimes cannot open a connection to one instance within the
timeout. That says nothing about the code under test, so a read moves on to
the next instance — and goes round the list twice — before giving up.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Callable, Sequence

from websockets.exceptions import InvalidHandshake
from websockets.sync.client import connect

from vgi_bluesky.jetstream import JETSTREAM_HOSTS

OPEN_TIMEOUT = 10.0
ROUNDS = 2


def read_raw(
    url_for: Callable[[str], str],
    n: int,
    seconds: float = 8.0,
    hosts: Sequence[str] = JETSTREAM_HOSTS,
) -> tuple[str, list[str | bytes]]:
    """Up to ``n`` messages from the first instance that accepts, and which one it was.

    ``url_for`` builds the URL for a host, so a test that must stay on one
    instance (a cursor is that instance's clock) can pass the returned host
    back as the only one.
    """
    failure: Exception | None = None
    for _ in range(ROUNDS):
        for host in hosts:
            try:
                socket = connect(url_for(host), open_timeout=OPEN_TIMEOUT, close_timeout=0.5, max_size=None)
            except (OSError, TimeoutError, InvalidHandshake) as exc:
                failure = exc
                continue
            messages: list[str | bytes] = []
            deadline = time.time() + seconds
            with socket:
                while len(messages) < n and time.time() < deadline:
                    with contextlib.suppress(TimeoutError):
                        messages.append(socket.recv(timeout=1))
            return host, messages
    raise ConnectionError(f"no Jetstream instance accepted a connection ({', '.join(hosts)}): {failure}")
