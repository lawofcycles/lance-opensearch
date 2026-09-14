# lance-opensearch

Distributed search layer over [Lance](https://github.com/lancedb/lance) tables for OpenSearch. Implementation of [RFC #22643](https://github.com/opensearch-project/OpenSearch/issues/22643).

Lance is an open columnar table format designed for machine learning workloads: multimodal columns, evolving embeddings, tables that receive concurrent writes from Ray or Spark, and time travel via manifest versions. What has been missing is an open source, distributed search layer on top of it. This plugin surfaces a Lance table as a shardable OpenSearch index without copying data out of Lance: OpenSearch's engine treats each Lance fragment as a Lucene leaf, delegates full-text and vector work to the indexes stored inside the Lance table, and follows the table forward as new manifest versions land.

## Status

Early draft. Not production ready. The read-side plumbing — namespace registration, mapping derivation, the four query shapes, and the follow-forward refresh loop — works end to end against real Lance tables, but interfaces and settings are still shifting. Use it to explore the design or to try it against your own Lance tables; not to run anything you depend on.

Built against OpenSearch 3.8.0 with Lance 11.0.0.

## Features

The plugin surfaces a Lance table as an OpenSearch index whose shards each own a subset of the table's fragments. Queries and gets are served directly out of the Lance columns and indexes.

- Namespace registration polls a directory for `*.lance` tables and surfaces each as an OpenSearch index. Mapping and shard count are derived from the Lance schema and row count.
- Full-text search over an FTS-indexed column (`match`, `bool` composition). BM25 scores are returned by Lance.
- Vector nearest-neighbour search via the custom `lance_knn` DSL query. Per-shard nearest scan; coordinator merge reconstructs the global top-k.
- Primary-key lookup via `GET /<index>/_doc/<id>`. Uses a Lance scalar index when present; falls back to a filtered scan.
- `_source` and `_id` synthesised on the fly from Lance rows.
- Aggregations through Lucene's aggregator over Lance-backed doc values.
- Automatic follow-forward when Lance advances to a new manifest version. No index close, no shard reallocation, no request downtime.

Mapping type coverage today: `int32` / `int64`, `boolean`, `date` / `timestamp`, `string` (as `lance_text` when the column has a Lance FTS index, otherwise `keyword`), `fixed_size_list<float>` (as `knn_vector`), `list<string>` (multi-valued `keyword`), `binary`.

## Not yet implemented

- Analytics route via `sandbox/plugins/analytics-backend-datafusion`. All queries currently take the reader route.
- PPL / SQL integration (lives in `opensearch-project/sql`).
- Mapping coverage for `ip`, `wildcard`, `object`, `nested`, and the geo family.
- Text analysis beyond Lance's native tokenizer. RFC's second text mode (OpenSearch analyzer → derived column backfill) is only proven for English.
- Native ingestion via `_bulk` / `_doc` (RFC future work #1).
- Lucene custom index type stored in `_indices/{uuid}/` (RFC future work #2).

Full backlog: the issues tab of this repository.

## Requirements

- Java 21
- OpenSearch 3.8.0
- macOS/aarch64, linux/x86_64, or linux/aarch64 (Lance's Rust native library ships in `org.lance:lance-core` for these platforms)

## Quick start

```
./gradlew build
```

produces the plugin zip at `build/distributions/opensearch-lance-0.1.0.zip`.

The full walkthrough — install into OpenSearch, prepare a Lance table, register a namespace, and run the four supported query shapes — is in [docs/getting-started.md](docs/getting-started.md).

## Known limitations

- Nearest-neighbour queries scan once per shard. When the same Lance table is spread across N shards, the same underlying data is scanned N times.
- Nearest-neighbour scores are `boost / (1 + distance)`, not metric-normalised. Compare scores within a single query, not across queries or engines.
- Fragment distribution is not size-aware; fragments are assigned to shards by `fragment_id % number_of_shards`. Heavy fragments concentrate on some shards.
- The engine is read-only. `_flush`, `_forcemerge`, `_settings` writes, `_close`, and `_open` on a Lance-backed index are either no-ops or unsupported; mutation happens on the Lance side.
- Namespace registrations are held in process memory. They need to be reissued after a cluster restart.
- REST catalogs (Glue, Unity, Iceberg REST) are not wired to the namespace endpoint yet; only the filesystem adapter is exercised today.
- If an OpenSearch index already exists under the same name as a surfaced Lance table, the plugin logs one warning and leaves the table alone on every subsequent poll. Rename, delete, or attach explicitly to resolve.

## Feedback

If you try this against your own Lance data, please open an issue with what you observed, which schemas failed to map, and which queries did or did not behave as expected. Design questions belong on the RFC thread ([opensearch-project/OpenSearch#22643](https://github.com/opensearch-project/OpenSearch/issues/22643)).

## License

Apache License 2.0. See [LICENSE.txt](LICENSE.txt).
