# Known limitations

Everything documented here is a shape that either falls through to the shard path, requires a workaround, or is deferred to a future ticket. Grouped by concern.

## Query shapes routed to the shard path

`LanceDispatchActionFilter.isDispatchable` refuses the following request shapes so the built-in shard path handles them instead of returning silently wrong results from the fragment executor:

- `collapse`, `rescore`, and pipeline aggregations (`avg_bucket`, `bucket_sort`, `cumulative_sum`, sibling and parent bucket-metrics variants). The fragment executor does not synthesise `CollapsingTopDocsCollector` state or the rescorer window, and the coordinator merge cannot replay pipeline aggregators without an "Already been replayed" error.
- `suggest`, `highlighter`, and `search_after` without a `sort` clause. Suggest and highlighter need Lance FTS internals the Java SDK does not surface; Lucene's `searchAfter` is only defined together with a sort.
- `min_score`, `terminate_after`, `track_total_hits: false` or an integer bound. Fragment path counts matches from Lance metadata (or Lucene `count`) without threading these knobs through, so the response envelope would ignore them. Shard path's `MinScoreCollector` / `EarlyTerminatingCollector` / total-hits-up-to gate handles the clipping.
- `stored_fields`, `docvalue_fields`, `explain`. Per-hit projection and `_explanation` are executor-side work the fragment path does not implement; shard path's built-in fetch phase handles all three.

Cross-index metrics also hit the shard path today.

## FTS query behaviour on stock `match` / `match_phrase`

OpenSearch's stock `match` and `match_phrase` queries against a `lance_text` field ignore `operator`, `minimum_should_match`, phrase order, and `slop`. Reason: `lance_text` uses a keyword-analyzer `TextSearchInfo` so the whole query string reaches Lance as a single token and Lance's own tokenizer runs on the query text — OpenSearch's combining layer never sees multiple tokens. Use the plugin's DSL queries for that control:

- `lance_match` for AND / OR operator, fuzziness, prefix length, max expansions.
- `lance_match_phrase` for phrase order and `slop`.
- `lance_multi_match` for multi-field search with per-field boosts.
- `lance_fts_boost` / `lance_fts_bool` to compose FTS clauses on Lance's side.

`minimum_should_match` on stock `match` is still ignored because Lance's Java SDK does not expose the equivalent knob today.

## Vector search

- `lance_knn` accepts only `Float32` element types. `int8`, `uint8`, `float16` vector columns are surfaced in the attach notes but excluded from the mapping until the Java SDK gains a matching `setKey` entry point.
- Nearest-neighbour scores are `boost / (1 + distance)`, not metric-normalised. Compare within a query, not across queries or engines.
- Nearest-neighbour queries scan once per Lance fragment. Many small fragments issue many native scans; compact with the Lance writer's compaction step to reduce the overhead.
- Filter clauses other than `match_all` / `term` / `terms` / `exists` / `range` / `bool` inside `lance_knn.filter` are rejected with 400 (no push-down to Lance).
- `lance_knn` combined with an outer `bool.filter` runs the outer clause as a post-filter (Lucene layer). Wrap it inside `lance_knn.filter` to push it into Lance.

## Shard model and concurrency

- Lance-backed indices are always single-shard. `POST /_lance/attach` rejects any request carrying `number_of_shards` with 400. Fragment path fans out per fragment regardless of shard count, so shards no longer influence search parallelism. Multi-node parallelism comes from replica placement (`index.number_of_replicas: data_nodes - 1`, or `auto_expand_replicas: 0-all`).
- Multi-node `_search` fan-out is pinned to the node hosting the primary shard because the fragment path does not sort-merge partial hits from replica hosts. Every fragment still executes: Lance fragments live in external storage, so the primary node can open all of them.
- `PUT /{index}` with `index.lance.table` in settings is rejected with 400 pointing at `POST /_lance/attach`. The engine can wire up without derivation via that path, but mapping ends up empty and every typed query fails; the reject prevents silent half-broken indices.
- Fragment path concurrency is capped by `lance.fragment_dispatch.max_concurrent` (default 4). Per-query heap (FTS score arrays sized by `maxDoc`, aggregation buffers) scales with concurrency rather than shard fan-out; without a cap, allocation can race the `lance_native` circuit breaker into `OutOfMemoryError` before the breaker fires. Requests over the limit block waiting for a permit.

## Primary key semantics

- `GET /<index>/_doc/<id>` requires the Lance table to carry `lance-schema:unenforced-primary-key` metadata on the PK column. Tables without a declared PK expose an empty `primary_key_field`; GET returns 404. `_search` still emits unique `_id` values synthesised as `<fragment>-<offset>`.
- Signed integer, UInt64, and Utf8 PKs are supported. Unsigned integer PKs narrower than 64 bits (UInt8 / UInt16 / UInt32) are not surfaced today because OpenSearch has no matching unsigned mapping type below `unsigned_long`.

## Freshness

- `_search` (fragment path) and `GET /_doc/{id}` (engine path) do not share a freshness view. After a Lance append advances the manifest, `_search` reflects the new rows on its next call because the fragment executor opens the latest version per query. Engine path (GET, stats) follows the shard's poll cadence (default 10s, configurable via `lance.namespace.poll_cadence`), so the same row can be visible to `_search` seconds before `GET` sees it. Read-your-writes depends on which API the caller used; prefer `_search` when the write side matters.
- `index.lance.uncovered_fragment_policy` (`wait` / `immediate`, default `immediate`) still exists in settings but both values expose the new version at once today. `wait` is reserved for a future async-optimize implementation.

## Storage and credentials

- `storage_options` are stored in plain index settings (`index.lance.storage_options.<key>`), including any credentials the operator writes there. Keystore / `SecureSetting` integration and the lance-namespace vended-credentials flow (`vend_credentials`, `expires_at_millis`) are not wired yet.
- Base-scoped options (`base_<url>.aws_access_key_id`) for nested Lance references are out of scope for now.
- REST catalogs (Glue, Unity, Iceberg REST) are not wired to the namespace endpoint yet. Only the filesystem adapter is exercised today. Sub-directory tables (`root/sub/table.lance`) are not surfaced either: the poller lists top-level tables only.
- Namespace registrations live in cluster state and survive full cluster restarts. Every node's poll cycle reads the same set. Multi-node runs no longer need each node to re-register the path independently.

## Mapping coverage gaps

- `ip`, `wildcard`, `object` (Arrow `Struct`), `nested` (Arrow `List<Struct>`), and the geo family are not surfaced.
- `Utf8` list, `Decimal`, and `FloatingPoint(HALF|DOUBLE)` columns are stored in the table but excluded from the mapping. The attach response notes them.
- Blob columns and `LargeBinary` beyond the basic `binary` mapping share the same status.
- Lindera and Jieba tokenizers are not bundled with lance-jni. Only ICU-based tokenization is available for CJK text through the Lance FTS index.

## Index lifecycle

- The engine is read-only. `_flush`, `_forcemerge`, `_settings` writes, `_close`, and `_open` on a Lance-backed index are either no-ops or unsupported; mutation happens on the Lance side.
- Automatic index builds happen only for tables at or under `lance.builder.max_rows` (default 1,000,000). Larger tables need `POST /_lance/build_indexes/{index}` explicitly, or a Lance-side build (Python `dataset.create_index`, Ray, Spark, Java SDK). Indexes built by the plugin still block subsequent `alter_columns` on the indexed column, so drop the index before altering the type.
- If an OpenSearch index already exists under the same name as a surfaced Lance table, the plugin logs one warning and leaves the table alone on every subsequent poll. Rename, delete, or attach explicitly to resolve.

## Reader memory profile

- Each Lance fragment leaf loads columns lazily. The reader constructor only performs a schema pass; a fragment that carries a deletion file additionally runs a `_rowaddr`-only scan to learn which physical rows are live. Every scalar column moves from "declared" to "loaded" the first time a Lucene accessor (doc values, sort, aggregation) asks for it, then stays in heap for the fragment's lifetime.
- `_id` and `_source` are not served from those whole-column loads. The fragment path fetches the rows behind the hits with a `_rowaddr IN (...)` take per leaf, so a `size:10` fetch reads ten rows of the projected columns regardless of table size. Queries that never render hits (aggregations, `size=0` hit counts) only heap-allocate the columns they consult.

## Version pinning

- `"version": N` on `POST /_lance/attach` pins a Lance-backed index to a specific manifest version for readonly snapshots. Tag and branch checkout are not exposed by the Lance Java SDK (v11 today), so they cannot be pinned from OpenSearch.

## Not yet implemented (RFC future work)

Items on the roadmap that no version of the plugin ships today.

- Analytics route via `sandbox/plugins/analytics-backend-datafusion`. Every query currently takes the reader route.
- PPL / SQL integration (lives in `opensearch-project/sql`).
- Text analysis beyond Lance's native tokenizer. The RFC's second text mode (OpenSearch analyzer plus a derived column backfilled by the plugin) is only proven for English today.
- Native ingestion via `_bulk` / `_doc` (RFC future work item 1).
- Lucene custom index type stored in `_indices/{uuid}/` (RFC future work item 2).

Full backlog: the issues tab of this repository.
