# lance-opensearch

An OpenSearch plugin that surfaces [Lance](https://github.com/lancedb/lance) tables as searchable indexes. Implementation of [RFC #22643](https://github.com/opensearch-project/OpenSearch/issues/22643).

Lance is an open columnar table format with multimodal columns, concurrent writers, and time travel via manifest versions. The format ships with vector and full text indexes, so vector search, full text search, and analytical aggregation can all run against the same table without extracting data or maintaining a parallel copy. This plugin registers a Lance table as a shardable OpenSearch index without copying data out of Lance: OpenSearch's engine treats each Lance fragment as a Lucene leaf, delegates full text and vector work to the indexes stored inside the Lance table, and follows the table forward as new manifest versions land.

## Status

Early draft. Not production ready. The read-side plumbing (namespace registration, mapping derivation, `_search` / GET / aggregation shapes, and the follow-forward refresh loop) works end to end against real Lance tables, but interfaces and settings are still shifting. Use it to explore the design or try it against your own Lance tables, not to run anything you depend on.

Built against OpenSearch 3.8.0 with Lance 12.0.0.

## What it does

- Registers a directory or a single Lance table URI and surfaces it as an OpenSearch index. Mapping derives from the Lance Arrow schema.
- Runs full text (`match`, `lance_match`, `lance_match_phrase`, `lance_multi_match`, `lance_fts_boost`, `lance_fts_bool`), vector (`lance_knn`), primary-key GET, and aggregation queries through a fragment fan-out that drives Lucene's stock aggregator machinery over per-fragment leaf readers.
- Follows the Lance manifest forward automatically. `"version": N` on attach also pins a readonly snapshot.
- Accepts per-table `storage_options` (S3, GCS, Azure) so a single JVM can address multiple buckets with different credentials.

See [docs/features.md](docs/features.md) for the full feature list and [docs/limitations.md](docs/limitations.md) for known limitations and shapes routed to the shard path.

## Requirements

- Java 21
- OpenSearch 3.8.0
- macOS/aarch64, linux/x86_64, or linux/aarch64 (Lance's Rust native library ships in `org.lance:lance-core` for these platforms)

## Build

```
./gradlew build
```

Produces `build/distributions/opensearch-lance-0.1.0.zip`.

To run the tests:

```
./gradlew test integTest multiNodeIntegTest
```

## Try it

The end-to-end walkthrough (install into OpenSearch, prepare a Lance table, register a namespace, run every query shape) is in [docs/getting-started.md](docs/getting-started.md).

## Documentation

- [docs/getting-started.md](docs/getting-started.md) — Build, install, prepare a table, run the query shapes.
- [docs/features.md](docs/features.md) — Feature reference by concern.
- [docs/limitations.md](docs/limitations.md) — Known limitations and shard-path fall-throughs.
- [CHANGELOG.md](CHANGELOG.md) — Release notes.

## Feedback

If you try this against your own Lance data, please open an issue with what you observed, which schemas failed to map, and which queries did or did not behave as expected. Design questions belong on the RFC thread ([opensearch-project/OpenSearch#22643](https://github.com/opensearch-project/OpenSearch/issues/22643)).

## License

Apache License 2.0. See [LICENSE.txt](LICENSE.txt).
