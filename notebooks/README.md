# Bluesky notebooks

[Does the average describe a typical post?](01_typical_posts_and_concentration.ipynb)
compares mean, median and concentration of likes on the official bsky.app account’s
own posts. The real worker retrieves data through Haybarn / VGI; visible SQL
materializes a bounded feed, defines the population and calculates the distribution.

## Run from this folder

Prerequisites: [uv](https://docs.astral.sh/uv/), Git and network access for first-time
package/extension installation. `uv` can provision Python. Copy this entire
`notebooks/` directory anywhere; no worker checkout or root environment is needed.

```bash
cd notebooks
uv sync --frozen
uv run --frozen python execute.py
```

The locked notebook environment uses Haybarn 2.0.0rc11. The visible ATTACH launches
remote Git revision `e7b043c1106e755b0083a4abbbbd66c49e145b9d` via `uvx`, Python 3.13
and SDK 0.51.1. The worker’s DuckDB filter evaluator remains in its separate runtime.
To upgrade, change the literal Git/SDK pins, then rerun replay, the experiment and
live mode before saving new outputs.

For an interactive kernel:

```bash
uv run --frozen python -m ipykernel install --user \
  --name vgi-bluesky-notebooks --display-name 'Python (vgi-bluesky notebooks)'
```

Open the notebook in your preferred Jupyter interface and select that kernel.
Run with this folder as the working directory. Setup, chart code and cleanup are
folded; parameters, connection SQL and analytical SQL remain visible.

## Data and boundaries

The default is **recorded public data**, not synthetic data. `data/official-feed.json`
contains a projected response from Bluesky’s public `getAuthorFeed` endpoint,
requested for `bsky.app`, `posts_no_replies`, no pins, limit 100, captured
2026-11-14 at 05:17:23 UTC. The response returned 98 feed items and an upstream
continuation cursor. Identifiers, timestamps, counters and reason types are retained;
post text, media, profile descriptions and the cursor are omitted. This intentionally
bounded recording is not a complete account history. Source metadata and projection
notes are stored beside the response. Bluesky content remains subject to the
repository’s [NOTICE](https://github.com/Query-farm/vgi-bluesky/blob/e7b043c1106e755b0083a4abbbbd66c49e145b9d/NOTICE).

`recorded_api.py` serves only that response on an ephemeral loopback port through
the worker’s supported endpoint override. It rejects unsupported requests and has
no upstream fallback. `DATA_MODE = "live"` clears endpoint overrides and calls
Bluesky directly, with the same 98-item output cap. It needs no account or credentials;
service errors fail visibly. Live counts and feed membership can change.

The creation window is start-inclusive/end-exclusive in UTC. Replay fixes its end
at capture time; live mode captures its end once before retrieval. The API excludes
replies, SQL removes reposts and deduplicates URIs, and the identity filter uses the
official account’s stable DID. NULL counters remain unavailable; zero is observed
zero. Creation time is client-supplied. Likes are cumulative counts at observation,
not unique people, exposure-adjusted quality, or likes gained during the window.

## Save the experiment separately

```bash
uv run --frozen python execute.py --parameters '{"LOOKBACK_DAYS":90}' \
  --output-dir validation/window-90
uv run --frozen python execute.py --parameters '{"DATA_MODE":"live"}' \
  --output-dir validation/live
```

The 90-day replay experiment expands only the creation window, keeping the response
and observation time fixed. Compare selected counts, mean, median and concentration;
reset to 30 days afterward. Live runs are independent observations, not controlled
comparisons. The runner uses a fresh temporary kernel, saves only successful output,
requires a separate destination for overrides and copies companions into that folder.
Logs, extracted chart images and experiment copies are ignored by Git. The main
notebook keeps its embedded successful outputs.
