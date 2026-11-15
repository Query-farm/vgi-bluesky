# Bluesky Social — executed SQL notebooks

[Worker repository](https://github.com/Query-farm/vgi-bluesky) · [Shared standard](https://github.com/Query-farm/vgi-example-notebooks/blob/main/docs/standard.md)

Read the executed notebooks on GitHub or use their **Open in Colab** buttons. Book 01 retains the original recorded-feed analysis; Book 02 is a separate, explicitly synthetic population example. Neither uses credentials or makes social writes.

| Notebook | Question | Experiment |
|---|---|---|
| [01 · Does the average describe a typical post?](01_typical_posts_and_concentration.ipynb) | How do mean, median and concentration describe a bounded feed? | Creation window 30 → 90 days, same recorded counters. |
| [02 · Engagement counts do not identify an audience](02_engagement_counts_do_not_identify_an_audience.ipynb) | How do counters, retained lists, visible replies and overlapping identities differ? | Requested reply depth 1 → 2, all other model inputs fixed. |

## Setup

Colab installs only the base shared package **v0.3.53**. Compatible preloaded data, plotting and kernel libraries remain in place. `prepare("bluesky")` retrieves four immutable, SHA256-verified companions: the original [recorded server](recorded_api.py) and [projected recording](data/official-feed.json), plus the separate [synthetic API model](synthetic_api.py) and [synthetic input](data/engagement-fixture.json). No manual clone is needed. For local execution from this directory:

```sh
uv sync --locked --extra runner
uv run --locked --extra runner vgi-notebooks
```

Install [uv](https://docs.astral.sh/uv/) and Git. Notebook Python supports 3.11–3.14; uv can provision the independently launched worker's Python 3.13 or newer. Initial shared-package, extension and worker installation requires network access. The visible release launch is:

```sql
INSTALL vgi FROM community;
LOAD vgi;
ATTACH 'bluesky' (TYPE vgi,
 LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-bluesky@v0.4.0 vgi-bluesky');
```

The release owns its dependencies; there are no transitive overrides, local worker binaries or checkout source requirements. Shared `connect(globals())` establishes UTC, empty temporary SQL-secret storage and `allow_persistent_secrets=False` before loading VGI. It prevents saved JupySQL INI connections from opening. The notebooks verify an empty secret inventory before ATTACH and cleanup, create no secrets, stop loopback servers and restore prior endpoint environment settings, with an exit cleanup fallback.

## Provenance and interpretation

**Book 01 — projected public recording.** [official-feed.json](data/official-feed.json) was captured **14 November 2026 at 05:17:23 UTC** from the public `app.bsky.feed.getAuthorFeed` endpoint for `bsky.app`, requesting `posts_no_replies`, no pinned posts and 100 items. The response contained 98 items with a continuation cursor. The stored projection retains identifiers, timestamps, counts and feed reasons; it removes text, media, profile descriptions and the cursor. It is neither original HTTP bytes nor synthetic data. The unchanged replay deliberately serves only those 98 rows, with no upstream fallback. Its SHA256 is `9c67bc977a9cb8faf04ff425a18e85df6139b620076360eb7ed7cf8fba8bd690`.

The original analytical SQL, substantive narrative and default displayed tables are preserved. The cap applies before selecting the account's own posts by stable DID; URI deduplication and the half-open creation-time window remain visible. Counts are cumulative at the recording time, not likes earned during that window. Missing counts remain missing. Creation timestamps are author supplied. A longer window cannot recover older rows beyond the recording; mean, median and concentration can move in either direction. Likes are not audience reach or a controlled quality score.

**Book 02 — authored synthetic model.** Every DID, `.example` handle, post and count is fictional. The minimal loopback server supports eight exact public GET contracts and never contacts upstream. Unsupported paths/parameters return an error; POST is unsupported. The real released worker performs parsing, list scanning, thread flattening and DID profile fan-out. This tests those transformations on known inputs; it does not establish live ranking, completeness, moderation or social behavior.

The model has two account candidates, two posts by one selected author, a post with 6 likes/3 reposts/3 quotes/4 replies, overlapping interaction sets, an old/current handle pair sharing a DID, a missing follower count, two visible direct replies, one nested reply and an unavailable node. SQL retains only three rows from each interaction list. The likes HTTP body contains six entries; “retained rows” is not a network-transfer claim. Quote rows are posts, not intrinsically unique authors. Repost interaction timestamps are NULL under the worker contract. Thread rows omit unavailable nodes; an unrequested generation is labeled unrequested, never zero. Profile enrichment preserves missing values and identity overlap; follower totals do not measure exposure.

## Separate experiments

```sh
uv run --locked --extra runner vgi-notebooks 01_typical_posts_and_concentration.ipynb --parameters '{"LOOKBACK_DAYS":90}' --output-dir validation/lookback-90
uv run --locked --extra runner vgi-notebooks 02_engagement_counts_do_not_identify_an_audience.ipynb --parameters '{"THREAD_DEPTH":2}' --output-dir validation/depth-2
```

The runner requires a separate destination for overrides and preserves successful defaults/experiments on failure. Defaults embed native SVG and content-addressed image links for GitHub; independent experiment copies retain native SVG. Review images, logs and experiment copies are ignored. One fresh preloaded Python 3.12 notebook-only execution tests the common launcher/registered companion path. Actual hosted Colab execution is not claimed.

## Optional public live mode

Book 01 supports an explicitly chosen `DATA_MODE = "live"`. It clears endpoint overrides before starting the same released worker, fixes one UTC observation time and reads at most 98 public feed rows. It never falls back to recorded or synthetic data. Use a separate output directory, for example:

```sh
uv run --locked --extra runner vgi-notebooks 01_typical_posts_and_concentration.ipynb --parameters '{"DATA_MODE":"live"}' --output-dir validation/live
```

The separate bounded live notebook was validated successfully through the released worker; its values are not included in the recorded default or controlled experiment. Live counters and feed membership change, so this is a new observation rather than a controlled parameter comparison. Public API availability and rate limits remain external constraints. Book 02 has no live mode: its identities, exact invariants and fixture expectations must not be sent to a real service. No social write, follow, message or account login is exposed by these examples.

## Coverage and unchanged-source checks

[coverage.json](coverage.json) records complete native function/table/view catalogs, raw-saved-notebook execution receipts and editorial mappings. There are **17 logical surfaces** in both release and captured current main: 16 functions plus the `trends` table; no views. All remain in the denominator, including the distinct `all_trends` function. The worker exposes public read methods and the read-only Jetstream stream; no authenticated/write SQL surfaces are present.

The two focused stories verify **9/17**, meeting the larger-catalog target of at least 50%: `author_feed`, `search_actors`, `search_posts`, `post`, `likes`, `reposted_by`, `quotes`, `thread`, `profile`. The eight explicit gaps are `actor_feeds`, `all_trends`, `feed`, `followers`, `follows`, `jetstream`, `popular_feeds` and `trends`. Current-interface credit is reconciled separately from exact released-runtime evidence.

Release **v0.4.0** resolves to `244db8e4f766d986863814e68b5c51a5f15d6bf3`; captured current public main is `eae45012647d936cc7deadeb48e115e246ce4744`. Runtime/dependency behavior is unchanged; metadata, examples and tasks differ. [Source CI 43561213091](https://github.com/Query-farm/vgi-bluesky/actions/runs/43561213091) passed structural catalog metadata, lint, offline tests and standalone entry-point startup. We reuse that unchanged-source evidence rather than rebuilding or rerunning full worker suites.

Separately, [scheduled live run 43600916694](https://github.com/Query-farm/vgi-bluesky/actions/runs/43600916694) had **2 failures, 11 passes**: Jetstream strict time ordering and replay-at-cursor timeout. These pre-existing live-stream failures are not notebook regressions, and Jetstream is not credited by these examples. Successful notebook execution does not imply that live-stream gate passed. Source code and root dependency locks are unchanged.
