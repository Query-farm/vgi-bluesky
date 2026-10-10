# Changelog

## 0.2.0

### Changed

- Errors carry vgi-rpc's error model (`error_code`, `error_kind`,
  `error_details`), so a client can tell a bad argument from a rate limit from
  an outage without reading the message:

  | Error | Code | Kind | Details |
  |---|---|---|---|
  | Unknown actor | `NOT_FOUND` | `bluesky.actor_not_found` | `ResourceInfo`; the "Did you mean" suggestion as `LocalizedMessage`; each match's profile as a `Help` link |
  | Malformed actor, post or feed reference; Jetstream limits | `INVALID_ARGUMENT` | `bluesky.invalid_argument` | `BadRequest` naming the argument |
  | Handle in a post/feed URL that resolves to nothing | `NOT_FOUND` | `bluesky.actor_not_found` | `ResourceInfo` |
  | Bluesky HTTP 400 / 401 / 403 / 404 / 429 / 5xx | `INVALID_ARGUMENT` / `UNAUTHENTICATED` / `PERMISSION_DENIED` / `NOT_FOUND` / `RESOURCE_EXHAUSTED` / `UNAVAILABLE` | — | `ErrorInfo` with the XRPC method and Bluesky's error name; `RetryInfo` on a 429 that says how long to wait |
  | Jetstream unreachable | `UNAVAILABLE` | — | — |

  Error classes keep their Python bases (`ValueError`, `ConnectionError`), so
  existing `except` clauses still match.
- Dependencies: vgi-python 0.43.1, vgi-rpc 0.50.1 (now declared directly),
  websockets 17.2; dev: ruff 0.17.


### Changed

- A wrong actor now fails with a clear error instead of a raw XRPC one, in
  `author_feed`, `followers`, `follows` and `actor_feeds`:
  `ActorNotFoundError: No Bluesky account 'medriscoll.bsky.social'. Did you mean
  'medriscoll.com' (Mike Driscoll)?` — the suggestion comes from one account
  search, made only when the lookup has already failed. A malformed actor
  raises `InvalidActorError` listing the accepted forms. Every other Bluesky
  error now reads `Bluesky <method> failed (HTTP <status>): <error>: <message>`
  rather than the raw JSON body.

### Documentation

- README recipes: posts from an author (latest, last week, most-liked, media
  only, on a topic, several authors, live by DID) and listening to the firehose
  for a topic (keyword, hashtag, replay, volume per minute, endless).
- A live topic listen from a DuckDB client also needs
  `SET enable_caching_operators = false`: `FILTER` caches output chunks of 64
  rows or fewer until it has a full vector, so a keyword match over an endless
  scan otherwise delivers nothing. Covered by an end-to-end test.

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
