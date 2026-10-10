Bluesky, meet SQL.

We've built vgi-bluesky: a read-only VGI worker that exposes Bluesky's public data as SQL tables and functions in Haybarn, Query.Farm's DuckDB-compatible distribution.

Explore posts, threads, profiles, followers and trending topics. Query the live Jetstream firehose. Join a burst of likes back to the posts they reference—all with SQL.

No Bluesky credentials needed. The worker reads public data and cannot post, like or follow.

Start the Haybarn CLI from your terminal:

uvx haybarn-cli

Then follow the README to load VGI, attach the worker and run the examples.

One query to try after attaching:

SELECT collection, count(*) AS events
FROM bluesky.jetstream()
GROUP BY ALL ORDER BY events DESC;

That gives you ten seconds of network activity, grouped by record type.

Code and examples: https://github.com/Query-farm/vgi-bluesky

What would you query first?

#Bluesky #SQL #DuckDB #OpenSource
