# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "vgi-python[http,duckdb]>=0.49.0",
#     "vgi-rpc>=0.53.1",
#     "httpx>=0.28.1",
#     "websockets>=17.2",
# ]
# ///
"""Stdio entry point for the Bluesky VGI worker (``uv run``).

ATTACH 'bluesky' (TYPE vgi, LOCATION 'uv run bluesky_worker.py');
"""

from __future__ import annotations

from vgi_bluesky.worker import main

if __name__ == "__main__":
    main()
