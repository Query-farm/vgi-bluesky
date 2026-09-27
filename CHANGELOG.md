# Changelog

## 0.1.0

First release: a read-only VGI worker over Bluesky's public AppView and its
Jetstream firehose, laid out after `vgi-kalshi`, on current dependencies
(vgi-python 0.37, vgi-rpc 0.47, Python 3.13+).

### Surface

- `trends` table (backed by `all_trends`).
- Lookups, blended and `LATERAL`-composable, batched 25 to a request:
  `profile`, `post`, `thread`.
- Listings, streamed one API page per tick: `search_actors`, `followers`,
  `follows`, `author_feed`, `likes`, `reposted_by`, `quotes`, `feed`,
  `popular_feeds`, `actor_feeds`.
- `search_posts`, one page of up to 100 hits, with filter pushdown on
  `author_handle`, `created_at` and `indexed_at`.
- `jetstream()` — the whole network's events via Jetstream. Bounded by `seconds`
  (default 10), `max_events`, a `LIMIT` or an upper bound on `event_time`, or
  endless with `seconds => 0`; batches close at `batch_events` or `batch_ms`.
  Filtered by `collections` (with `.*` wildcards) and `dids`, or by pushed-down
  `collection`/`did` equality; replayable over the ~36 hours Jetstream buffers.
  `cursor` resumes *after* the `time_us` it names, so chained polls are gapless
  and duplicate-free, and a call with a cursor never fails over to another
  instance.

### Findings baked into the design

- `searchPosts` is refused (403) on `public.api.bsky.app` and served
  anonymously by `api.bsky.app` — but only its first page; any anonymous
  request carrying a cursor is also a 403.
- `getPosts` answers 500 to a handle-authority AT-URI, so post references are
  canonicalised to DIDs first.
- vgi-python evaluates pushed-down filters with an in-process DuckDB engine, so
  it is installed with its `haybarn` extra.
- Jetstream's cursor is inclusive, `time_us` is strictly increasing, and a
  collection filter still delivers identity and account events.
- websockets' `close()` waits its full 10-second timeout against a live
  firehose; sockets close on a background thread with a half-second timeout.
- DuckDB clients hold a streaming result until `streaming_buffer_size` (~1 MB)
  fills; `SET streaming_buffer_size = '1KB'` before consuming a live
  `jetstream()` scan.
- The AppView compresses with gzip and ignores Brotli, so no Brotli dependency.
- vgi-lint 0.82 requires agent-task graders to live outside the catalog, in
  `vgi-agent-tests.yaml`.
