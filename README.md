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
- `lance_knn` accepts an inner `filter` clause that Lance evaluates before applying the K-nearest cutoff (a pre-filter, so K matching rows are still returned when they exist). Supported filter clauses are `match_all`, `term`, `terms`, `exists`, `range`, and `bool` (`filter` / `must` / `must_not` / `should`). Other clauses such as `match`, geo queries, and scripts are rejected with 400. `lance_knn` combined with an outer `bool.filter` still runs the outer clause as a post-filter; wrap it inside `lance_knn.filter` to push it into Lance.
- `lance_knn` is limited to `Float32` element types. Lance vectors declared as `int8`, `uint8` / binary, or `float16` are surfaced in the attach notes but excluded from the mapping until the Java SDK gains a `setKey(byte[])` / `setKey(short[])` entry point.
- GET `/_doc/{id}` only works when the index has a single shard. The plugin returns 400 for GET on multi-shard indices; use `_search` with a term query on the primary key column instead, or reattach the index with `number_of_shards: 1`.
- GET by `_id` requires a primary key declared through the Lance `lance-schema:unenforced-primary-key` metadata. Tables without a declared PK expose an empty `primary_key_field` and GET returns 404.
- Fragment distribution is not size-aware; fragments are assigned to shards by `fragment_id % number_of_shards`. Heavy fragments concentrate on some shards.
- The engine is read-only. `_flush`, `_forcemerge`, `_settings` writes, `_close`, and `_open` on a Lance-backed index are either no-ops or unsupported; mutation happens on the Lance side.
- Namespace registrations are held in process memory on the node that received the request. They need to be reissued after a cluster restart, and other nodes in a multi-node cluster will not surface the same tables until they too register the path.
- REST catalogs (Glue, Unity, Iceberg REST) are not wired to the namespace endpoint yet; only the filesystem adapter is exercised today. Tables placed under sub-directories (`root/sub/table.lance`) are not surfaced either — the poller lists top-level tables only.
- If an OpenSearch index already exists under the same name as a surfaced Lance table, the plugin logs one warning and leaves the table alone on every subsequent poll. Rename, delete, or attach explicitly to resolve.
- Automatic index builds are disabled by default. Attach and namespace registration do not create FTS / scalar / vector indexes on the Lance table; call `POST /_lance/build_indexes/{index}` explicitly to build them. Indexes created that way still block subsequent Lance `alter_columns` calls on the indexed column, so drop the index via `drop_index` before altering the type.
- Each leaf reader eagerly materialises every scalar column of its fragment into heap on first refresh. Large tables therefore need JVM heap proportional to the working row count times the per-row footprint of the scalar columns.
- S3-compatible storage is configured through node-level environment variables (`AWS_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `AWS_ALLOW_HTTP`). Per-table `storage_options` sent through attach or namespace bodies are not accepted yet.
- Lance `blob` columns and `LargeBinary` / `Struct` / `Utf8` list / `Decimal` / `FloatingPoint(HALF|DOUBLE)` columns are stored in the table but not surfaced in the mapping or `_source`. The attach response notes them so operators can plan around the gap.
- Version pinning (`version` / `tag` / `branch`) is not exposed at the OpenSearch layer yet; the plugin always reads the latest committed Lance version.
- Lindera and Jieba tokenizers are not bundled with lance-jni. Only ICU-based tokenization is available for CJK text through the Lance FTS index.
- Lance's index cache / metadata cache / Session sharing configuration is not exposed as node settings yet; the plugin uses SDK defaults.

## Feedback

If you try this against your own Lance data, please open an issue with what you observed, which schemas failed to map, and which queries did or did not behave as expected. Design questions belong on the RFC thread ([opensearch-project/OpenSearch#22643](https://github.com/opensearch-project/OpenSearch/issues/22643)).

## License

Apache License 2.0. See [LICENSE.txt](LICENSE.txt).
