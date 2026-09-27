<p align="center">
  <a href="https://query.farm/vgi/">
    <img src="https://raw.githubusercontent.com/Query-farm/vgi-bluesky/main/docs/vgi-logo.png" alt="Vector Gateway Interface logo" width="320">
  </a>
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

<a href="https://bsky.app"><img align="right" src="https://raw.githubusercontent.com/Query-farm/vgi-bluesky/main/docs/bluesky-logo.svg" alt="Bluesky" width="72"></a>

No credentials needed. Everything comes from [Bluesky](https://bsky.app)'s
public API and its Jetstream firehose, and the worker can't post, like, follow
or change anything.

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
> **Streaming to a DuckDB client? Change two settings first:**
>
> ```sql
> SET streaming_buffer_size = '1KB';        -- don't wait for ~1 MB of results
> SET enable_caching_operators = false;     -- don't hold back small filtered chunks
> SELECT * FROM bluesky.jetstream(seconds => 0, batch_ms => 500);
> ```
>
> DuckDB holds back a streaming result until about 1 MB has built up, which the
> firehose's small rows take a long time to fill: with the default, an endless
> scan showed nothing for 90 seconds. And a filter that keeps only a few rows
> per batch, like a keyword match, produces small chunks that DuckDB caches until
> it has 2048 rows or the scan ends, so a live topic listen showed nothing at all.
> With both settings, rows arrive about once a second (a filtered listen takes a
> few seconds longer to start). Queries that aggregate (`count(*)`, `GROUP BY`)
> or that end on their own don't need either.

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

## Recipes

### Posts from an author

```sql
-- Their latest posts, without replies or reposts
SELECT created_at, text, like_count, post_url
FROM bluesky.author_feed('bsky.app', filter => 'posts_no_replies')
WHERE reason IS NULL
LIMIT 20;

-- The last week. author_feed runs newest first, so cap it and filter the result
SELECT created_at, text, like_count
FROM (SELECT * FROM bluesky.author_feed('jay.bsky.team') LIMIT 200)
WHERE reason IS NULL AND created_at > now() - INTERVAL 7 DAY
ORDER BY created_at DESC;

-- Their most-liked recent posts
SELECT text, like_count, repost_count, post_url
FROM (SELECT * FROM bluesky.author_feed('jay.bsky.team', filter => 'posts_no_replies') LIMIT 300)
WHERE reason IS NULL
ORDER BY like_count DESC LIMIT 10;

-- Only posts with images or video
SELECT created_at, text, image_count, post_url
FROM bluesky.author_feed('bsky.app', filter => 'posts_with_media')
LIMIT 10;

-- What they've said about a topic, via search
SELECT created_at, text, post_url
FROM bluesky.search_posts('duckdb', author => 'duckdb.org');

-- Several authors at once: listings take literal arguments, so combine them
SELECT author_handle, created_at, text
FROM (SELECT * FROM bluesky.author_feed('bsky.app', filter => 'posts_no_replies') LIMIT 10)
UNION ALL
SELECT author_handle, created_at, text
FROM (SELECT * FROM bluesky.author_feed('jay.bsky.team', filter => 'posts_no_replies') LIMIT 10)
ORDER BY created_at DESC;
```

`reason IS NULL` drops reposts, which appear in an author's feed as the original
author's post. `filter` also accepts `posts_with_replies` (the default),
`posts_and_author_threads` and `posts_with_video`.

To catch an author's new posts as they happen, use the firehose with their DID
(from `profile()`). The firehose identifies accounts by DID, not handle:

```sql
SELECT event_time, text, uri
FROM bluesky.jetstream(collections => 'app.bsky.feed.post',
                       dids => 'did:plc:z72i7hdynmk6r22z27h6tvur',   -- bsky.app
                       seconds => 300);
```

### Listening to the firehose for a topic

Jetstream filters by record type and account, not by content. So listen to all
posts and match the topic in the `WHERE` clause: the whole network's posts flow
through the worker, and DuckDB keeps the matches.

```sql
-- Posts mentioning a keyword, over the next minute
SELECT event_time, did, text, uri
FROM bluesky.jetstream(collections => 'app.bsky.feed.post', seconds => 60)
WHERE operation = 'create' AND text ILIKE '%duckdb%';

-- Posts with a hashtag (no '#')
SELECT event_time, did, text
FROM bluesky.jetstream(collections => 'app.bsky.feed.post', seconds => 60)
WHERE list_contains(hashtags, 'art');

-- Look back instead of waiting: the last 10 minutes, replayed in ~10 seconds
SELECT event_time, did, text
FROM bluesky.jetstream(collections => 'app.bsky.feed.post', seconds => 120)
WHERE event_time >= now() - INTERVAL 10 MINUTE AND event_time < now()
  AND text ILIKE '%duckdb%';

-- How much a topic is being discussed, minute by minute
SELECT date_trunc('minute', event_time) AS minute, count(*) AS posts
FROM bluesky.jetstream(collections => 'app.bsky.feed.post', seconds => 120)
WHERE event_time >= now() - INTERVAL 10 MINUTE AND event_time < now()
  AND text ILIKE '%election%'
GROUP BY minute ORDER BY minute;
```

To keep listening until you stop the query, use `seconds => 0` with the two
settings above:

```sql
SET streaming_buffer_size = '1KB';
SET enable_caching_operators = false;

SELECT event_time, did, text, uri
FROM bluesky.jetstream(collections => 'app.bsky.feed.post', seconds => 0)
WHERE text ILIKE '%duckdb%';
```

A niche topic can go minutes without a post, so an empty live window isn't
unusual. Replay a longer window to see whether it's being discussed at all. The
replay's `seconds` must cover the time to read it: 10 minutes of posts took
about 10 seconds.

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
