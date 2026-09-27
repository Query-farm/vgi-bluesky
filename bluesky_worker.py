# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "vgi-python[http,haybarn]>=0.37.3",
#     "vgi-rpc>=0.47.2",
#     "httpx>=0.28.1",
#     "websockets>=17.1",
# ]
# ///
"""Stdio entry point for the Bluesky VGI worker (``uv run``).

ATTACH 'bluesky' (TYPE vgi, LOCATION 'uv run bluesky_worker.py');
"""

from __future__ import annotations

from vgi_bluesky.worker import main

if __name__ == "__main__":
    main()
