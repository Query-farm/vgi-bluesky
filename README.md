<p align="center">
  <a href="https://query.farm/vgi/">
    <img src="https://raw.githubusercontent.com/Query-farm/vgi-bluesky/main/docs/vgi-logo.png" alt="Vector Gateway Interface logo" width="320">
  </a>
</p>

<h1 align="center">vgi-bluesky</h1>

<p align="center">
  <a href="https://bsky.app">Bluesky</a> as ordinary DuckDB tables — posts, threads, profiles,<br>
  the follow graph, likes, reposts, quotes, custom feeds, trending topics,<br>
  and the live firehose via Jetstream.<br>
  A <strong>read-only</strong> <a href="https://query.farm/vgi/">VGI</a> worker, built by <a href="https://query.farm">🚜 Query.Farm</a>
</p>

<p align="center">
  <a href="https://github.com/Query-farm/vgi-bluesky/actions/workflows/ci.yml"><img src="https://github.com/Query-farm/vgi-bluesky/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT"></a>
  <img src="https://img.shields.io/badge/python-3.13%2B-blue.svg" alt="Python 3.13+">
  <a href="https://query.farm/vgi/"><img src="https://img.shields.io/badge/VGI-Vector%20Gateway%20Interface-2f7d32.svg" alt="VGI"></a>
</p>

---

> **No credentials required, and none accepted.** Everything here is public:
> Bluesky's AppView serves it anonymously, and Jetstream needs no login. Every
> write in the AT Protocol — posting, liking, following, even logging in — is an
> XRPC *procedure* sent as `POST`. This worker has one HTTP chokepoint (`_get` in
> `bluesky_api.py`, the only module that imports `httpx`), which issues `GET`
> only and refuses any method not on an allow-list of lexicon *queries*. The
> firehose is read over a WebSocket that is only ever received on
> (`jetstream.py`, the only module that imports `websockets`). A CI guard
> (`tests/test_readonly_guard.py`) fails the build if any of that stops being true.

```sql
ATTACH 'bluesky' (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-bluesky vgi-bluesky');

-- What is trending right now
SELECT rank, display_name, category, post_count FROM bluesky.trends ORDER BY rank;

-- Who is talking about DuckDB, and how big their audience is
SELECT p.handle, p.followers_count
FROM (SELECT did FROM bluesky.search_actors('duckdb') LIMIT 20) a,
     LATERAL bluesky.profile(a.did) p
ORDER BY p.followers_count DESC;

-- Ten seconds of the entire network, by record type
SELECT collection, count(*) AS events
FROM bluesky.jetstream()
GROUP BY ALL ORDER BY events DESC;
```

## Run

```bash
uv run bluesky_worker.py            # stdio
uv run serve.py --port 8000         # HTTP
```

```sql
ATTACH 'bluesky' (TYPE vgi, LOCATION 'uv run bluesky_worker.py');
```

Both scripts carry PEP-723 headers pinning their dependencies, so they run from
a fresh clone with nothing installed. That `LOCATION` resolves
`bluesky_worker.py` against the working directory, though, so it only works from
inside the clone.

Anywhere else, point the `LOCATION` at this repository directly. `uvx` fetches
and caches the worker on first use — nothing to install, and the working
directory stops mattering:

```sql
ATTACH 'bluesky' (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-bluesky vgi-bluesky');
```

Pin a tag for a deployment, so the worker cannot change under you:

```sql
ATTACH 'bluesky' (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-bluesky@v0.1.0 vgi-bluesky');
```

**This package is not published to PyPI, and is not intended to be** — install
it from this repository. Its *dependencies* are all published, so `uvx` resolves
them normally.

To run the worker as a server and attach to a URL instead, `vgi-bluesky-http` is
the HTTP entry point:

```sql
ATTACH 'bluesky' (TYPE vgi, LOCATION 'http://localhost:8000');
```

### Developing

```bash
git clone https://github.com/Query-farm/vgi-bluesky
cd vgi-bluesky

uv sync --all-extras     # Install dependencies
uv run pytest            # 316 offline tests
uv run ruff check .      # Lint
```

Dependencies resolve from PyPI, so a fresh clone works with no sibling
checkouts. To develop against a local `vgi-python` or `vgi-rpc` instead, install
them over the top — this keeps the path out of the committed manifest, where it
would break the clone for everyone else:

```bash
uv pip install -e ../vgi-python -e ../vgi-rpc
```

The entry-point scripts (`bluesky_worker.py`, `serve.py`) always resolve from
PyPI via their own PEP-723 headers, independently of `pyproject.toml`. That is
deliberate — and it is why `tests/test_packaging.py` exists, since the two sets
of pins drift silently otherwise.

## Surface

Names are bare — they are already qualified by the `bluesky` catalog.

Every **actor** argument accepts a handle (`bsky.app`), a DID (`did:plc:…`),
`@handle`, or a pasted `https://bsky.app/profile/…` URL. Every **post** or
**feed** argument accepts an AT-URI (`at://…/app.bsky.feed.post/…`) or the
`https://bsky.app/profile/…/post/…` URL the app shows. Store and join on `did`:
a handle is a DNS name the account can change at any time.

### Table

| Table | |
|---|---|
| `trends` | What Bluesky currently reports as trending — up to 25 rows, in rank order |

`trends` is a real catalog table, not a function: it has no key, it is small,
and it is one unpaginated request. It is backed by the `all_trends` function via
`Table(function=…)`, which VGI auto-wires into a scan.

### Functions

Functions come in three shapes, and which one an object gets is decided by the
endpoint behind it.

A **lookup** is blended (`RowTransformFunction`): the fetch is bounded, so one
registration serves a literal call, a scalar subquery and a correlated
`LATERAL`. A whole input batch is answered together — profiles and posts 25 to
a request — so a `LATERAL` over 100 actors is four calls, not a hundred.

A **listing** sits on a cursor-paged endpoint. It is a stateful scan that emits
one API page (100 rows) per tick, holding the cursor in VGI scan state, so a
`LIMIT` stops early and rows arrive immediately. That matters more here than
most places: `bsky.app` has over 35 million followers. The cost is that its
argument must be a **literal** — DuckDB rejects a subquery there, and a
streaming scan cannot be the inner side of a `LATERAL`. Look the key up first,
then call the listing with it.

The **stream** is `jetstream()` — see [The firehose](#the-firehose).

| Function | Shape | Positional | Named |
|---|---|---|---|
| `profile(actor)` | lookup | actor | |
| `post(uri)` | lookup | post | |
| `thread(uri)` | lookup | post | `depth`, `parent_height` |
| `search_actors(query)` | listing | query | |
| `followers(actor)` | listing | actor | |
| `follows(actor)` | listing | actor | |
| `author_feed(actor)` | listing | actor | `filter`, `include_pins` |
| `search_posts(query)` | one page | query | `sort`, `author`, `mentions`, `lang`, `domain`, `url`, `tag`, `since`, `until`, `cache_ttl` |
| `likes(uri)` | listing | post | |
| `reposted_by(uri)` | listing | post | |
| `quotes(uri)` | listing | post | |
| `feed(uri)` | listing | feed | |
| `popular_feeds()` | listing | | `query` |
| `actor_feeds(actor)` | listing | actor | |
| `jetstream()` | stream | | `collections`, `dids`, `seconds`, `max_events`, `cursor`, `batch_events`, `batch_ms` |

```sql
-- An account's recent original posts and how they did (reposts excluded)
SELECT created_at, text, like_count, repost_count
FROM bluesky.author_feed('bsky.app', filter => 'posts_no_replies')
WHERE reason IS NULL LIMIT 20;

-- The most-liked direct replies in a conversation (depth < 0 are its ancestors)
SELECT author_handle, text, like_count
FROM bluesky.thread('https://bsky.app/profile/bsky.app/post/3l6oveex3ii2l', depth => 1)
WHERE depth = 1 ORDER BY like_count DESC LIMIT 10;

-- Which hashtags an account uses most
SELECT tag, count(*) AS uses
FROM (SELECT unnest(hashtags) AS tag
      FROM (SELECT hashtags FROM bluesky.author_feed('bsky.app') LIMIT 300))
GROUP BY tag ORDER BY uses DESC;
```

**Counts versus listings.** A viral post has hundreds of thousands of likes.
`profile()` carries `followers_count`, `follows_count` and `posts_count`;
`post()` carries `like_count`, `repost_count`, `reply_count`, `quote_count` and
`bookmark_count`. Reach for `followers()` or `likes()` only when you need the
individual accounts, and put a `LIMIT` on them — an aggregate such as
`count(*)` over a large account walks every page.

## The firehose

`jetstream()` reads [Jetstream](https://github.com/bluesky-social/jetstream),
Bluesky's JSON re-encoding of the AT Protocol firehose: every post, like,
repost, follow, block and profile change on the network, as it happens — about
400 events a second, roughly two thirds of them likes. No credentials are
needed.

```sql
-- New posts from across the network, live, for ten seconds (the default)
SELECT event_time, did, text, langs
FROM bluesky.jetstream(collections => 'app.bsky.feed.post')
WHERE operation = 'create';

-- Posting volume by language over twenty seconds
SELECT lang, count(*) AS posts
FROM (SELECT unnest(langs) AS lang
      FROM bluesky.jetstream(collections => 'app.bsky.feed.post', seconds => 20))
GROUP BY lang ORDER BY posts DESC;

-- The most-liked posts of the last 30 seconds, hydrated through post()
SELECT p.author_handle, p.text, j.likes
FROM (SELECT subject_uri, count(*) AS likes
      FROM bluesky.jetstream(collections => 'app.bsky.feed.like', seconds => 30)
      WHERE subject_uri LIKE '%/app.bsky.feed.post/%'
      GROUP BY subject_uri ORDER BY likes DESC LIMIT 5) j,
     LATERAL bluesky.post(j.subject_uri) p
ORDER BY j.likes DESC;
```

### What a row is

One event per row. Nearly all are `kind = 'commit'`: a record created, updated
or deleted in someone's repository, with `collection` saying what kind —
`app.bsky.feed.post`, `app.bsky.feed.like`, `app.bsky.feed.repost`,
`app.bsky.graph.follow`, `app.bsky.graph.block`, `app.bsky.actor.profile` and
more. The whole record is in `record` as JSON (`record->>'$.subject.uri'`); the
post columns (`text`, `langs`, `hashtags`, `mentions`, `links`, `embed_type`, …)
are filled for posts, and `subject_uri` / `subject_did` for likes, reposts,
follows and blocks. A few rows are `identity` or `account` events.

Events are raw: no handles, no like counts. Join a post's `uri` to `post()` or
a `did` to `profile()` under a `LATERAL` to hydrate them, as above.

### How long it runs

By default a call **ends**, at the first of:

- `seconds` of wall-clock listening (default 10);
- `max_events` events read;
- a `LIMIT`;
- an upper bound on `event_time` / `time_us` in the `WHERE`.

With **`seconds => 0` it never ends on its own**: it streams until the caller
stops reading or cancels the query. Either way, each tick emits one batch,
closed after `batch_events` events (default 1000) or `batch_ms` milliseconds
(default 1000), whichever comes first — an empty batch if the stream was quiet.
Over HTTP that is one request per batch. A `LIMIT` or a cancellation lands
within one batch, and the abandoned Jetstream socket is closed once it has been
idle for two batch windows plus five seconds.

### Streaming to a DuckDB client: set `streaming_buffer_size`

> [!IMPORTANT]
> Before consuming an endless (or long) `jetstream()` scan from DuckDB, shrink
> the client's streaming buffer:
>
> ```sql
> SET streaming_buffer_size = '1KB';
> SELECT * FROM bluesky.jetstream(seconds => 0, batch_ms => 500);
> ```

DuckDB's streaming result keeps executing a query until
`streaming_buffer_size` of results — **976.5 KiB by default** — has accumulated,
or the query ends, before it hands anything to the client. A native scan fills
that instantly; a firehose delivering a few hundred small rows a second does
not. With the default, `SELECT time_us FROM bluesky.jetstream(seconds => 0)`
delivered **nothing at all in 90 seconds**, and a 6-second scan delivered its
first row to the client at 6.0 s.

The rows were never stuck in the worker or in the vgi extension. A UDF recording
when each row passed through DuckDB's pipeline saw ~350 rows *every second*
while the client saw none — and the same with `vgi_table_buffering`,
`vgi_split_scans` and `vgi_result_cache` each turned off. With
`streaming_buffer_size = '1KB'`, batches reach the client about once a second,
over both stdio and HTTP `ATTACH`; `tests/test_end_to_end.py` holds that to
account.

The worker cannot set this for you — it is a client-side setting — so it is
also written into `jetstream()`'s own catalog documentation, where an agent
reading the catalog will find it. Bounded scans that are aggregated (`count(*)`,
`GROUP BY`) do not need it: their result only exists once the scan ends anyway.

### Filters and replay

`collections` (comma-separated, with prefix wildcards such as
`'app.bsky.graph.*'`) and `dids` become Jetstream's own filters, as do
`WHERE collection = …` and `WHERE did = …`, so unwanted events never cross the
network. That translation is safe because it is a superset: `identity` and
`account` events still arrive under a collection filter, and the predicate is
re-applied to every row. Jetstream accepts at most 100 collections; 101 is an
HTTP 400 at the handshake, which the worker reports by name instead.

Jetstream buffers roughly **the last day and a half**. `cursor =>
epoch_us(now() - INTERVAL 10 MINUTE)`, or simply a lower bound on `event_time`
in the `WHERE`, starts there; an upper bound becomes the stopping rule, which is
safe because `time_us` is strictly increasing within a stream. So a range
replays exactly that window and stops:

```sql
SELECT count(*)
FROM bluesky.jetstream(collections => 'app.bsky.feed.post', seconds => 60)
WHERE event_time >= now() - INTERVAL 3 MINUTE
  AND event_time <  now() - INTERVAL 2 MINUTE;
```

A cursor older than the buffer silently starts at the oldest buffered event.

### Polling continuously

`time_us` is Jetstream's cursor. Pass the highest `time_us` you have read back
as `cursor`, and the next call resumes with the event after it:

```sql
-- First page: the last five minutes, up to 1000 events
SELECT * FROM bluesky.jetstream(cursor => epoch_us(now() - INTERVAL 5 MINUTE), max_events => 1000);
-- Every later page: resume after the highest time_us from the previous one
SELECT * FROM bluesky.jetstream(cursor => 1790545579803644, max_events => 1000);
```

- **`cursor` means *after*.** Jetstream's own cursor is inclusive — an event's
  own `time_us` returns that event first — so feeding it back unchanged would
  repeat one event per poll. The worker adds the microsecond; for a timestamp
  that shifts the start by one microsecond and nothing else.
- **Gapless, verified.** A fixed window replayed as one 3000-event read and as
  three chained 1000-event polls returned identical events, with no duplicates.
  `test_chained_polls_equal_one_continuous_read` repeats that comparison.
- **Use `cursor =>`, not `WHERE time_us > …`.** `max_events` counts events read
  *before* any `WHERE`, so the `WHERE` form spends one of them on the event you
  already have (ask for 200, get 199).
- **Cursors belong to one instance.** Each Jetstream server stamps its own
  `time_us`. A call with a cursor never fails over to another instance — it
  retries its own and then fails loudly, rather than resuming inexactly. Set
  `BLUESKY_JETSTREAM_URL` to choose the instance when you poll across runs.

`time_us` is the only resumable position Jetstream offers. Commits carry
`did` + `rev` and a record `uri` + `cid`, which identify a change but have no
network-wide order; the relay's global `seq` appears only on identity and
account events.

### Resilience

The scan's state holds the last `time_us`. The open WebSocket is pooled per scan
as an optimisation only: a tick that finds it gone — a dropped connection, or
the scan moved to another process over HTTP — reconnects to the same instance
from that cursor and drops any replayed overlap. A failed connect is retried
with backoff (~0.5 s → 4 s) before the scan gives up, so a long-running stream
survives a Jetstream restart without losing events.

## Design notes

**Two hosts.** `public.api.bsky.app` is a CDN-fronted read replica of the
AppView and serves everything here anonymously — except `searchPosts`, which it
answers with **HTTP 403**. The main AppView host, `api.bsky.app`, serves search
anonymously, so search alone goes there. Both are overridable
(`BLUESKY_APPVIEW_URL`, `BLUESKY_SEARCH_URL`).

**Search returns one page.** `api.bsky.app` serves the first page of search
results anonymously but answers **403 to any anonymous request carrying a
cursor**. Following it would turn every search past 100 hits into an error, so
`search_posts` stops after one page by design: at most 100 posts per call.
Narrow with `since`/`until`, `author`, `lang` or `tag` and `UNION ALL` several
calls to see more. `tests/test_live.py` pins this, so the day Bluesky changes
it, a test says so.

**Handles in post URIs are resolved first.** `getPosts` answers **HTTP 500** to
an AT-URI whose authority is a handle (`at://bsky.app/…`), though
`getPostThread` accepts the same URI. Every post and feed reference is therefore
canonicalised to a DID-authority AT-URI through
`com.atproto.identity.resolveHandle`, once per handle per batch.

**Search pushdown is careful about time.** `WHERE author_handle = '…'` becomes
the endpoint's `author` filter. Time predicates become `since`/`until` only
where that is provably no narrower, because the endpoint filters on `sortAt` —
the *earlier* of a post's `createdAt` and `indexedAt` — not on either column:

- an upper bound on either column bounds `sortAt` too, so `until` is pushed,
  widened by a second since `until` is exclusive and `<=` is not;
- a lower bound on `created_at` alone says nothing about `sortAt` — a
  future-dated post has a `sortAt` below its `createdAt` — so it is *not*
  pushed. Pushing it would drop that post, and nothing downstream could recover
  it. Only when both columns are bounded below is `since` pushed.

Bounds from several columns are compared as instants, not strings — as strings,
`…:00Z` sorts after `…:00.500000Z`.

**Declaring pushdown is a promise to apply it.** Once a function accepts filter
pushdown, DuckDB drops its own filter above the scan, so every predicate a
function receives is applied to the rows by the worker
(`pushdown.build_filtered`), not merely translated where it can be. Evaluating
those predicates needs an in-process DuckDB engine, which is why vgi-python is
installed with its **`haybarn` extra**: without it the worker imports and binds
fine, and then every filtered scan fails at runtime with *"No DuckDB-compatible
engine is installed"*. Only a real `ATTACH` found that; `tests/test_packaging.py`
now guards it.

**Timestamps.** A post's `created_at` is whatever the author's app wrote — it
can be backdated, and is occasionally not a representable date at all. Values a
nanosecond-resolution client cannot hold (outside 1678–2262) become NULL rather
than a result that raises when materialised. `indexed_at`, when the AppView
first saw the post, is the clock to trust for ordering.

**Every conversion is total.** One malformed value in one post — a non-integer
count, a list of the wrong shape, a garbage timestamp — becomes NULL in that
cell rather than failing the batch and every other row in it.

**Closing a WebSocket used to cost ten seconds.** websockets' `close()` waits up
to `close_timeout` (10 s by default) for the server's close frame, and against a
firehose that is still pushing it waits the whole time — 10.00 s on every one of
five measured closes, which appeared as ten seconds of dead time at the end of a
scan. Sockets are closed with a half-second timeout on a background thread, so
no query waits on the goodbye.

**Transport.** The AppView compresses with gzip and ignores Brotli — offered
only `br`, it sent one page of an author feed uncompressed (698 KB against 88 KB
gzipped) — so no Brotli dependency is carried. A process-wide httpx client is
reused across the pages of a scan rather than paying a TLS handshake per page.

`_get` retries five times with exponential backoff (~0.5 s → 8 s), honouring a
`Retry-After` up to 30 s, on a 429, a transient 5xx and a dropped connection — a
`GET` is idempotent. A 4xx is not retried.

## Caching

The origin's own headers drive the policy — nothing is invented. Its
`Cache-Control` is forwarded to DuckDB's result cache, so if Bluesky changes a
TTL, this follows automatically.

| Source | Bluesky sends | We advertise |
|---|---|---|
| `public.api.bsky.app` (everything but search) | `public, max-age=30` | `ttl=30`, per-value for lookups |
| `api.bsky.app` (`search_posts`) | *(nothing)* | uncached unless `cache_ttl` |
| error responses | `public, max-age=5` | never folded in |
| Jetstream | *(a live stream)* | never cached |

A response counts as cacheable only if it declares a non-zero `max-age` and
nothing that forbids reuse (`no-store`, `no-cache`, `private`); the shortest
lifetime across a call's responses wins. Lookups memoise per input value, so a
`LATERAL` that repeats an actor is a cache hit. Every cacheable result carries
`stale_if_error=300`, so a failed refetch serves briefly stale rather than
failing the query.

## Catalog metadata

Everything a client sees on `ATTACH` — object descriptions, column comments,
result schemas, examples, categories — is published as `vgi.*` tags and checked
by [vgi-lint](https://github.com/Query-farm/vgi-lint-check), which scores it
**100/100 with every shipped example executed** against the live API:

```bash
vgi-lint lint                     # config lives in vgi-lint.toml
vgi-lint lint --audit-waivers     # prove the waiver still buys something
```

Column documentation has a single source: `vgi_bluesky/schemas.py` attaches a
comment to every Arrow field via `meta.field()`, and `meta.result_columns_schema()`
reads those same strings back out to build each function's declared result
schema. A column documented once therefore shows up in `DESCRIBE`, in
`duckdb_columns()`, and in the function's `vgi.result_columns_schema` — and
cannot drift between them.

The agent-suitability suite publishes only `{name, prompt}` per task in
`vgi.agent_test_tasks`. The graders — reference SQL and success criteria — live
in `vgi-agent-tests.yaml`, so an agent under test cannot read the answers out of
the catalog. Every object is exercised by at least one task.

One rule is waived, in `vgi-lint.toml`, with a recorded kind and reason that
`--audit-waivers` re-checks: VGI311 asks that a parameterless scan be exposed
as a table, which `all_trends` is — as `trends`. The rule matches on name, and
the names differ deliberately because a function and a table cannot share one
in a schema.

## CI

`.github/workflows/ci.yml` runs on every push: ruff (lint and format), the
offline tests, a check that both entry-point scripts start without the dev
checkouts, and `vgi-lint`'s structural tier with `--audit-waivers`. All of it
resolves from PyPI (`UV_NO_SOURCES=1`), so it also proves the published
dependencies are sufficient.

`.github/workflows/live.yml` runs daily, never concurrently, and is the half
that touches Bluesky: the live API and Jetstream tests, then the end-to-end SQL
suite against a real `ATTACH`, then `vgi-lint`'s behavioural tier, which runs
every shipped example against the real API. It is deliberately not on push —
Bluesky rate-limits anonymous traffic per IP, so concurrent runs throttle each
other into failures that say nothing about the code.

## Tests

```bash
pytest              # 316 offline tests
pytest -m live      # 28 tests against the public API, Jetstream and a real ATTACH
```

`tests/test_end_to_end.py` runs on [Haybarn](https://pypi.org/project/haybarn/),
the DuckDB distribution the `vgi` extension is published for; set
`VGI_EXTENSION` to load a local build instead. It is the tier that found the
missing filter engine, and it holds the firehose to its promises: a budget is a
budget, a `LIMIT` ends a 60-second listen early, a replayed window stays inside
its bounds, chained polls equal one continuous read, and an endless scan
streams to the client.

`tests/test_live.py` pins each API behaviour the design depends on — the page
and batch limits, the 403 on the public host's search, the 403 on a search
cursor, the 500 on a handle-authority URI, `time_us` strictly increasing, a
collection filter being a superset, replay starting at the cursor — so a change
on Bluesky's side names the assumption it broke.

`tests/test_readonly_guard.py` asserts the read-only property structurally: no
write-shaped HTTP call anywhere, `httpx` and `websockets` each confined to one
module, no procedure names in the source, and a chokepoint that refuses any
method off its allow-list before a request is made.

## License

Copyright © 2026 [Query Farm LLC](https://query.farm)

Released under the **MIT License** — see [LICENSE](LICENSE).

The posts and profiles this worker returns belong to their authors and are
served by Bluesky Social PBC under [Bluesky's terms of
service](https://bsky.social/about/support/tos); they are not covered by that
license. See [NOTICE](NOTICE). This project is not affiliated with or endorsed
by Bluesky Social PBC.

---

<p align="center">
  Built with <a href="https://query.farm/vgi/">VGI — the Vector Gateway Interface</a><br>
  by <a href="https://query.farm">🚜 Query.Farm</a>
</p>
