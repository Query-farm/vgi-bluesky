<p align="center">
  <a href="https://bsky.app"><img src="https://raw.githubusercontent.com/Query-farm/vgi-bluesky/main/docs/bluesky-logo.svg" alt="Bluesky logo" height="96"></a>
  &nbsp;&nbsp;&nbsp;&nbsp;
  <a href="https://query.farm/vgi/"><img src="https://raw.githubusercontent.com/Query-farm/vgi-bluesky/main/docs/vgi-logo.png" alt="Vector Gateway Interface logo" height="96"></a>
</p>

<h1 align="center">vgi-bluesky</h1>

<p align="center">
  <a href="https://bsky.app">Bluesky</a> in DuckDB: posts, threads, profiles, followers,<br>
  likes, feeds, trending topics and the live firehose, as SQL tables.<br>
  A <strong>read-only</strong> <a href="https://query.farm/vgi/">VGI</a> worker, built by <a href="https://query.farm">🚜 Query.Farm</a>
</p>

<p align="center">
  <a href="https://github.com/Query-farm/vgi-bluesky/actions/workflows/ci.yml"><img src="https://github.com/Query-farm/vgi-bluesky/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT"></a>
  <img src="https://img.shields.io/badge/python-3.13%2B-blue.svg" alt="Python 3.13+">
  <a href="https://query.farm/vgi/"><img src="https://img.shields.io/badge/VGI-Vector%20Gateway%20Interface-2f7d32.svg" alt="VGI"></a>
</p>

---

No credentials needed. Everything comes from Bluesky's public API and its
Jetstream firehose, and the worker can't post, like, follow or change anything.

```sql
ATTACH 'bluesky' (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-bluesky vgi-bluesky');

-- What's trending right now
SELECT rank, display_name, category, post_count FROM bluesky.trends ORDER BY rank;

-- Accounts about DuckDB, ranked by audience
SELECT p.handle, p.followers_count
FROM (SELECT did FROM bluesky.search_actors('duckdb') LIMIT 20) a,
     LATERAL bluesky.profile(a.did) p
ORDER BY p.followers_count DESC;

-- Ten seconds of the whole network, by record type
SELECT collection, count(*) AS events
FROM bluesky.jetstream()
GROUP BY ALL ORDER BY events DESC;
```

## Install

`uvx` fetches and caches the worker on first use; there is nothing to install.
Pin a tag so the worker can't change under you:

```sql
ATTACH 'bluesky' (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-bluesky@v0.1.0 vgi-bluesky');
```

From a clone, run it directly, or serve it over HTTP and attach by URL:

```bash
uv run bluesky_worker.py            # stdio: LOCATION 'uv run bluesky_worker.py'
uv run serve.py --port 8000         # HTTP:  LOCATION 'http://localhost:8000'
```

The package isn't published to PyPI; install it from this repository.

## Tables and functions

| Name | What it returns |
|---|---|
| `trends` *(table)* | Bluesky's trending topics, up to 25, in rank order |
| `profile(actor)` | An account: bio, follower, following and post counts, verification |
| `post(uri)` | A post with its current like, repost, reply and quote counts |
| `thread(uri)` | A whole conversation, one row per post, with `depth` relative to the post |
| `search_posts(query)` | Full-text post search, up to 100 posts per call |
| `search_actors(query)` | Account search |
| `author_feed(actor)` | An account's posts and reposts, newest first |
| `followers(actor)` / `follows(actor)` | The follow graph, newest first |
| `likes(uri)` / `reposted_by(uri)` / `quotes(uri)` | Who engaged with a post |
| `feed(uri)` | What a custom feed is serving |
| `popular_feeds()` / `actor_feeds(actor)` | Find custom feeds |
| `jetstream()` | The live firehose — see [below](#the-firehose) |

Arguments accept what you'd paste: an actor can be a handle (`bsky.app`), a DID,
`@handle` or a profile URL; a post or feed can be an AT-URI or a `bsky.app` URL.
Handles can change, so join on `did`.

```sql
-- An account's recent posts and how they did, reposts excluded
SELECT created_at, text, like_count, repost_count
FROM bluesky.author_feed('bsky.app', filter => 'posts_no_replies')
WHERE reason IS NULL LIMIT 20;

-- The most-liked direct replies to a post
SELECT author_handle, text, like_count
FROM bluesky.thread('https://bsky.app/profile/bsky.app/post/3l6oveex3ii2l', depth => 1)
WHERE depth = 1 ORDER BY like_count DESC LIMIT 10;
```

**Good to know:**

- **Lookups compose; listings don't.** `profile`, `post` and `thread` work
  inside a `LATERAL` and batch their requests. The listing functions stream
  page by page, so a `LIMIT` stops them early, but they need a literal argument,
  not a subquery or a column.
- **Use counts, not listings, for totals.** `bsky.app` has over 35 million
  followers. `profile()` and `post()` carry the counts; `count(*)` over
  `followers()` would page through all of them.
- **Search returns at most 100 posts per call.** Bluesky refuses to page
  anonymous searches further. Narrow with `since`, `until`, `author`, `lang` or
  `tag` and combine several calls.
- **Most results are cached for 30 seconds**, as the API instructs, so
  repeating a query within that window costs nothing. Search and the firehose
  are live; `search_posts` takes `cache_ttl` if you want caching.

## The firehose

`jetstream()` reads [Jetstream](https://github.com/bluesky-social/jetstream),
Bluesky's live feed of every post, like, repost, follow and block on the
network — about 400 events a second.

```sql
-- New posts from across the network, for ten seconds
SELECT event_time, did, text, langs
FROM bluesky.jetstream(collections => 'app.bsky.feed.post')
WHERE operation = 'create';

-- The most-liked posts of the last 30 seconds, with author and text
SELECT p.author_handle, p.text, j.likes
FROM (SELECT subject_uri, count(*) AS likes
      FROM bluesky.jetstream(collections => 'app.bsky.feed.like', seconds => 30)
      WHERE subject_uri LIKE '%/app.bsky.feed.post/%'
      GROUP BY subject_uri ORDER BY likes DESC LIMIT 5) j,
     LATERAL bluesky.post(j.subject_uri) p
ORDER BY j.likes DESC;
```

Each row is one event. `collection` says what kind (`app.bsky.feed.post`,
`app.bsky.feed.like`, `app.bsky.graph.follow`, …), `record` holds the full
record as JSON, and posts get their own columns (`text`, `langs`, `hashtags`, …).
Events carry DIDs, not handles; join to `profile()` or `post()` for those.

**How long it runs.** By default, ten seconds. It stops at the first of
`seconds`, `max_events`, a `LIMIT`, or an upper bound on `event_time`. With
`seconds => 0` it streams until you stop it. Rows arrive in batches, closed
every `batch_events` events (default 1000) or `batch_ms` milliseconds (default
1000), whichever comes first.

> [!IMPORTANT]
> **Streaming to a DuckDB client? Shrink its buffer first:**
>
> ```sql
> SET streaming_buffer_size = '1KB';
> SELECT * FROM bluesky.jetstream(seconds => 0, batch_ms => 500);
> ```
>
> DuckDB holds back a streaming result until about 1 MB has built up. The
> firehose's small rows take a long time to fill that: with the default, an
> endless scan showed nothing for 90 seconds. With `1KB`, rows arrive every
> second. Aggregates (`count(*)`, `GROUP BY`) don't need this, since their
> result only exists once the scan ends.

**Filtering.** `collections` (comma-separated, wildcards like
`'app.bsky.graph.*'` allowed) and `dids` filter at the source, as does
`WHERE collection = …` or `WHERE did = …`.

**Replay.** Jetstream keeps about the last 36 hours. A time range replays
exactly that window, then stops:

```sql
SELECT count(*)
FROM bluesky.jetstream(collections => 'app.bsky.feed.post', seconds => 60)
WHERE event_time >= now() - INTERVAL 3 MINUTE
  AND event_time <  now() - INTERVAL 2 MINUTE;
```

**Polling without gaps.** `time_us` is the resume point. Pass the highest one
you've read as `cursor`, and the next call starts right after it:

```sql
SELECT * FROM bluesky.jetstream(cursor => epoch_us(now() - INTERVAL 5 MINUTE), max_events => 1000);
SELECT * FROM bluesky.jetstream(cursor => 1790545579803644, max_events => 1000);  -- next page
```

Chained calls neither skip nor repeat events. Use `cursor =>` rather than
`WHERE time_us > …`, which spends one of `max_events` on the event you already
have. Each Jetstream server keeps its own clock, so when polling across runs,
set `BLUESKY_JETSTREAM_URL` to stay on one server.

## Development

Setup, design notes, caching, CI and tests are in
[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).

## License

Copyright © 2026 [Query Farm LLC](https://query.farm). Released under the
[MIT License](LICENSE).

Posts and profiles belong to their authors and are served by Bluesky Social PBC
under [Bluesky's terms of service](https://bsky.social/about/support/tos); they
aren't covered by the MIT License. The Bluesky name and logo are Bluesky's
trademarks, used only to identify the service. This project isn't affiliated
with or endorsed by Bluesky Social PBC. See [NOTICE](NOTICE).

---

<p align="center">
  Built with <a href="https://query.farm/vgi/">VGI — the Vector Gateway Interface</a><br>
  by <a href="https://query.farm">🚜 Query.Farm</a>
</p>
