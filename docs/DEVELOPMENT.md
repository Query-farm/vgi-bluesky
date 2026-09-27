# Developing vgi-bluesky

Notes for working on the worker: setup, design decisions and the evidence behind
them, caching, catalog metadata, CI and tests. For using it, see the
[README](../README.md).

## Setup

```bash
git clone https://github.com/Query-farm/vgi-bluesky
cd vgi-bluesky

uv sync --all-extras     # install dependencies
uv run pytest            # 316 offline tests
uv run pytest -m live    # 28 tests against the public API, Jetstream and a real ATTACH
uv run ruff check .      # lint
```

Dependencies resolve from PyPI, so a fresh clone needs no sibling checkouts. To
develop against a local `vgi-python` or `vgi-rpc`, install over the top rather
than adding a path source to `pyproject.toml`, which would break the clone for
everyone else:

```bash
uv pip install -e ../vgi-python -e ../vgi-rpc
```

The entry-point scripts (`bluesky_worker.py`, `serve.py`) resolve their
dependencies from their own PEP-723 headers, independently of
`pyproject.toml`. The two lists drift silently, which is why
`tests/test_packaging.py` compares them.

## Read-only by construction

Every write in the AT Protocol — posting, liking, following, even logging in —
is an XRPC *procedure* sent as `POST`. The worker has one HTTP chokepoint
(`_get` in `bluesky_api.py`, the only module that imports `httpx`). It issues
`GET` only and refuses any method not on an allow-list of lexicon queries. The
firehose is read over a WebSocket that is only ever received on (`jetstream.py`,
the only module that imports `websockets`). `tests/test_readonly_guard.py`
enforces all of this structurally.

## Design notes

**Three function shapes.** Lookups (`profile`, `post`, `thread`) are blended
`RowTransformFunction`s: bounded fetches that serve a literal call, a scalar
subquery and a correlated `LATERAL`, batched 25 to a request. Listings sit on
cursor-paged endpoints and are stateful `TableFunctionGenerator` scans that
emit one page per tick, so a `LIMIT` stops early. The trade-off is that their
argument must be a literal: DuckDB rejects a subquery there, and a streaming
scan cannot be the inner side of a `LATERAL`. `jetstream()` is a stream.

**Two hosts.** `public.api.bsky.app` (a CDN-fronted AppView replica) serves
everything anonymously except `searchPosts`, which it refuses with HTTP 403.
`api.bsky.app` serves search anonymously, so search alone goes there. Both are
overridable with `BLUESKY_APPVIEW_URL` and `BLUESKY_SEARCH_URL`.

**Search returns one page.** `api.bsky.app` returns 403 to any anonymous search
request carrying a cursor, so `search_posts` stops after one page of up to 100
posts. `tests/test_live.py` pins this, so a change on Bluesky's side fails a
test.

**Handles in post URIs are resolved first.** `getPosts` answers HTTP 500 to an
AT-URI with a handle authority (`at://bsky.app/…`), though `getPostThread`
accepts it. Post and feed references are canonicalised to DID-authority URIs
through `com.atproto.identity.resolveHandle`, once per handle per batch.

**Search pushdown is careful about time.** `WHERE author_handle = '…'` becomes
the `author` filter. The endpoint filters time on `sortAt`, the *earlier* of
`createdAt` and `indexedAt`, so:

- an upper bound on either column bounds `sortAt`, and is pushed as `until`
  (widened a second, since `until` is exclusive);
- a lower bound on `created_at` alone is *not* pushed — a future-dated post has
  a `sortAt` below its `createdAt`, and pushing would drop it. `since` is pushed
  only when both columns are bounded below.

Bounds are compared as instants, not strings (`…:00Z` sorts after
`…:00.500000Z`).

**Declaring pushdown is a promise to apply it.** Once a function accepts filter
pushdown, DuckDB drops its own filter above the scan, so the worker applies
every predicate to the rows (`pushdown.build_filtered`). That needs an
in-process DuckDB engine, hence vgi-python's `haybarn` extra: without it,
filtered scans fail at runtime with *"No DuckDB-compatible engine is
installed"*. Only a real `ATTACH` caught this; `tests/test_packaging.py` now
guards it.

**Conversions are total.** A malformed value — a non-integer count, a
wrongly-shaped list, an unrepresentable timestamp — becomes NULL in its cell
rather than failing the batch. Post `created_at` values are client-written and
can be backdated; those a nanosecond-resolution client cannot hold (outside
1678–2262) become NULL.

**Transport.** The AppView compresses with gzip and ignores Brotli (a page sent
698 KB uncompressed when only `br` was offered, 88 KB gzipped), so no Brotli
dependency. A process-wide httpx client is reused across a scan's pages. `_get`
retries 429s, transient 5xx and dropped connections five times with backoff
(~0.5 s → 8 s), honouring `Retry-After` up to 30 s; 4xx is not retried.

### Jetstream

- **Measured behaviour.** About 400 events a second; `time_us` strictly
  increasing within a connection; the cursor inclusive (an event's own
  `time_us` returns that event first); `identity`/`account` events delivered
  despite a collection filter; replay buffered for roughly 36 hours; more than
  100 collections refused with HTTP 400. `tests/test_live.py` pins each one.
- **Cursor semantics.** Because Jetstream's cursor is inclusive, `cursor` means
  *after*: the worker adds a microsecond. Chained polls were verified to return
  exactly the events of one continuous read (`tests/test_end_to_end.py`).
- **Instance clocks.** Each Jetstream server stamps its own `time_us`, so a scan
  stays on the instance it started on, and a call with a cursor never fails
  over.
- **State and pooling.** Scan state holds the last `time_us`; the WebSocket is
  pooled per scan only as an optimisation. A tick that finds it gone reconnects
  from the cursor and drops the replayed overlap. Connects retry with backoff
  (~0.5 s → 4 s). Abandoned sockets are reaped after two batch windows plus
  five seconds — scaled per scan, since a fixed limit shorter than `batch_ms`
  would close a live socket mid-batch.
- **Closing.** websockets' `close()` waits `close_timeout` (10 s by default) for
  the server's close frame, and a firehose that is still pushing makes it wait
  the whole time — measured at 10.00 s on five of five closes. Sockets close
  with a half-second timeout on a background thread.
- **DuckDB's streaming buffer.** A DuckDB client receives nothing from a
  streaming result until `streaming_buffer_size` (976.5 KiB) accumulates or the
  query ends. A UDF timing rows through the pipeline showed ~350 rows a second
  flowing while the client saw none; turning off `vgi_table_buffering`,
  `vgi_split_scans` and `vgi_result_cache` changed nothing, and
  `SET streaming_buffer_size = '1KB'` fixed it over stdio and HTTP alike.

## Caching

The origin's `Cache-Control` is forwarded to DuckDB's result cache, never
invented:

| Source | Bluesky sends | Advertised |
|---|---|---|
| `public.api.bsky.app` | `public, max-age=30` | `ttl=30`, per-value for lookups |
| `api.bsky.app` (search) | nothing | uncached unless `cache_ttl` |
| error responses | `public, max-age=5` | never folded in |
| Jetstream | a live stream | never cached |

A response is cacheable only with a non-zero `max-age` and nothing forbidding
reuse (`no-store`, `no-cache`, `private`); the shortest lifetime in a call
wins. Cacheable results carry `stale_if_error=300`.

## Catalog metadata

Descriptions, column comments, result schemas, examples and categories are
published as `vgi.*` tags and checked by
[vgi-lint](https://github.com/Query-farm/vgi-lint-check):

```bash
vgi-lint lint                     # config in vgi-lint.toml
vgi-lint lint --audit-waivers     # prove the waiver still applies
```

Column documentation has one source: `schemas.py` attaches a comment to every
Arrow field via `meta.field()`, and `meta.result_columns_schema()` reads the same
strings back for each function's declared result schema, so `DESCRIBE`,
`duckdb_columns()` and `vgi.result_columns_schema` cannot drift apart.

Agent test tasks publish only `{name, prompt}` in `vgi.agent_test_tasks`; the
graders live in `vgi-agent-tests.yaml`, out of the catalog. Every object is
exercised by at least one task.

One rule is waived in `vgi-lint.toml`: VGI311 wants a parameterless scan exposed
as a table, which `all_trends` is — as `trends`. The rule matches on name, and a
function and a table cannot share one.

## CI

- **`ci.yml`, every push:** ruff (lint and format), offline tests, entry points
  starting without the dev checkouts, and vgi-lint's structural tier. Resolves
  from PyPI (`UV_NO_SOURCES=1`).
- **`live.yml`, daily and never concurrent:** live API and Jetstream tests, the
  end-to-end SQL suite, then vgi-lint executing every shipped example. Not on
  push, because Bluesky rate-limits anonymous traffic per IP.

## Tests

- **`test_end_to_end.py`** runs SQL through a real `ATTACH` on
  [Haybarn](https://pypi.org/project/haybarn/), the DuckDB distribution the
  `vgi` extension is published for (set `VGI_EXTENSION` to load a local build).
  It found the missing filter engine and holds Jetstream to its behaviour:
  budgets, early `LIMIT`, replay windows, gapless polling, endless streaming.
- **`test_live.py`** pins every API behaviour the design relies on, so a change
  on Bluesky's side names the assumption it broke.
- **`test_readonly_guard.py`** asserts the read-only property: no write-shaped
  HTTP calls, `httpx` and `websockets` each confined to one module, no procedure
  names in the source, and a chokepoint that refuses unlisted methods before
  any request.
