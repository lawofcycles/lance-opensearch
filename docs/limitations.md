# Known limitations

Everything documented here is a shape that either falls through to the shard path, requires a workaround, or is deferred to a future ticket. Grouped by concern.

## Query shapes routed to the shard path

`LanceDispatchActionFilter.isDispatchable` refuses the following request shapes so the built-in shard path handles them instead of returning silently wrong results from the fragment executor:

- `collapse`, `rescore`, and pipeline aggregations (`avg_bucket`, `bucket_sort`, `cumulative_sum`, sibling and parent bucket-metrics variants). The fragment executor does not synthesise `CollapsingTopDocsCollector` state or the rescorer window, and the coordinator merge cannot replay pipeline aggregators without an "Already been replayed" error.
- `suggest`, `highlighter`, and `search_after` without a `sort` clause. Suggest and highlighter need Lance FTS internals the Java SDK does not surface; Lucene's `searchAfter` is only defined together with a sort.
- `min_score`, `terminate_after`. Fragment path counts matches from Lance metadata (or Lucene `count`) without threading these knobs through, so the response envelope would ignore them. Shard path's `MinScoreCollector` / `EarlyTerminatingCollector` handles the clipping.
- `stored_fields`, `docvalue_fields`, `explain`. Per-hit projection and `_explanation` are executor-side work the fragment path does not implement; shard path's built-in fetch phase handles all three.

Cross-index metrics also hit the shard path today.

A request Lance refuses as invalid input (for example `lance_match_phrase` on an FTS index built without `with_position: true`) answers 400 `illegal_argument_exception` with Lance's message on the fragment path. On the shard path the same request answers 500: Lucene's query phase wraps the failure in `QueryPhaseExecutionException`, which OpenSearch reports as a server error, and the plugin does not intercept that phase. Lance's message is still in the response body.

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

- Lance-backed indices are always single-shard. `POST /_lance/attach` rejects any request carrying `number_of_shards` with 400. Fragment path fans out per fragment to every data node regardless of shard count or replica settings, so neither influences search parallelism; a replica copy of a Lance-backed index adds nothing to query throughput.
- Fragments are assigned round-robin by fragment id without regard to size, and a request fails as a whole when any one node fails. The fan-out covers every data node in cluster state, so the plugin has to be installed on all of them; a data node without the plugin fails the request with `ActionNotFoundTransportException`. On a node without a shard copy the executor creates a temporary `IndexService` per request through `IndicesService.withTempIndexService` (analyzer construction and mapping parse; 3.5 to 8 ms per request on the four column test fixture once the node has served one, about 100 ms for the very first request on a freshly started node, independent of row count). It is not cached: OpenSearch offers no public way to keep an unregistered `IndexService` alive beyond that call, and a registered one is closed by the cluster state applier as soon as it sees the node holds no shard of the index.
- `PUT /{index}` with `index.lance.table` in settings is rejected with 400 pointing at `POST /_lance/attach`. The engine can wire up without derivation via that path, but mapping ends up empty and every typed query fails; the reject prevents silent half-broken indices.
- Fragment path concurrency is capped by `lance.fragment_dispatch.max_concurrent` (default 4). Per-query heap (FTS score arrays sized by `maxDoc`, aggregation buffers) scales with concurrency rather than shard fan-out; without a cap, allocation can race the `lance_native` circuit breaker into `OutOfMemoryError` before the breaker fires. Requests over the limit block waiting for a permit.
- When a reader wrapper is installed on the index (the security plugin's DLS / FLS wrapper), `hits.total.value` and `_count` are counted from the wrapped reader's live docs on every request shape: `match_all` in any spelling reads the liveDocs bitset, everything else collects the query's scorer under those liveDocs. The Lance metadata counts (`Dataset.countRows`, `Fragment.countRows`) and the count-only Lance scans that serve unwrapped indexes are not used, so a filtered user's count costs a pass over the matching rows instead of a metadata read.
- Full-text queries (`lance_match` and the other Lance FTS shapes) scan Lance without a fragment list on every node. Lance 12 turns a fragment list into a `_rowid` prefilter read over the listed fragments, so a node that executes a proper subset of the fragments (several data nodes) looks the whole table up from the inverted index and keeps the rows of its own fragments by the fragment id in `_rowaddr`; the lookup is repeated once per node but does not depend on the row count. For shapes that need every match (aggregations, sort by a field, post_filter, `size 0`, `track_total_hits: true`) that lookup is a probe capped at `lance.fts.subset_probe_limit` rows (default 1,000,000): when a query matches more rows than the limit, a node holding a proper subset discards the probe and repeats the scan with its fragment list, so that shape falls back to the prefilter read and its latency grows with the row count of the subset. Removing the prefilter read itself needs a Lance-side change (a fragment-bitmap prefilter instead of a row id read).

## Primary key semantics

- `GET /<index>/_doc/<id>` requires the Lance table to carry `lance-schema:unenforced-primary-key` metadata on the PK column. Tables without a declared PK expose an empty `primary_key_field`; GET returns 404. `_search` still emits unique `_id` values synthesised as `<fragment>-<offset>`.
- Signed integer, UInt64, and Utf8 PKs are supported. Unsigned integer PKs narrower than 64 bits (UInt8 / UInt16 / UInt32) are not surfaced today because OpenSearch has no matching unsigned mapping type below `unsigned_long`.

## Freshness

- `_search` (fragment path) and `GET /_doc/{id}` (engine path) do not share a freshness view. After a Lance append advances the manifest, `_search` reflects the new rows on its next call because the coordinator reads the latest manifest version per query and the executors build a snapshot for it. Engine path (GET, stats) follows the shard's poll cadence (default 10s, configurable via `lance.namespace.poll_cadence`), so the same row can be visible to `_search` seconds before `GET` sees it. Read-your-writes depends on which API the caller used; prefer `_search` when the write side matters.
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

## Stats and monitoring APIs

`_cat/indices`, `_cat/shards`, `{index}/_stats`, `_nodes/stats/indices`, and `_cluster/stats` report a Lance-backed index as follows.

- `docs.count` and `docs.deleted` are the Lance row count and deletion count of the manifest version the shard currently serves (they advance on refresh, so a Lance write shows up after the next poll or an explicit `_refresh`). `_cluster/health` reports green for the single primary shard; it turns red only when the shard fails to open the table (unreachable path, missing credentials, dropped table), because there is no replica to fall back to.
- `store.size`, `pri.store.size`, and `store.size_in_bytes` are the size of the shard's Lucene directory, which holds only the bootstrap commit (a few hundred bytes), not the Lance data. OpenSearch computes the store size from the shard directory and offers no engine-level override. The manifest-recorded data file total is available internally as `DocsStats.totalSizeInBytes` (used by `_rollover` size conditions); data files written without a `file_size_bytes` manifest entry contribute zero to that total, and Lance index files (`Index.getSizeBytes()`) are not included anywhere.
- Segment fields (`segments.count`, `segments.memory`, the per-shard list in `_segments`) and the write-side groups `indexing`, `merges`, `flush`, `translog.operations`, `warmer`, and `recovery` are always 0 or empty. There are no Lucene segments and no OpenSearch-side writes; the values are not synthesised from Lance metadata. `refresh.total` also stays 0: Lance version advances swap the reader without going through the shard's refresh listeners, and `search.*` stays 0 because `_search` is answered by the fragment path, not the shard.
- `suggest.*`, `completion.*`, `fielddata.*`, `query_cache.*`, `request_cache.*`, `memory.total`, and the matching `_stats` cache groups stay 0 as well: the fragment path never reaches the shard-level caches or suggesters. `get.*` is the one request counter that does move: `get.total` counts `GET /_doc/{id}` on a table with a declared primary key (the only request shape the shard engine serves itself).

## Reader memory profile

- Each Lance fragment leaf loads columns lazily. On the fragment path the leaf is a view over a cached table snapshot (see features.md, "Snapshot and column cache"): numeric, boolean, date, float, keyword and keyword array columns it reads for the whole fragment live off-heap in the node's column store and are shared across requests. A keyword load interns the fragment's values in heap while the scan runs and releases that once the dictionary and ordinals are written off-heap. Two keyword shapes are still built per request in heap and dropped when the request ends: the dictionary of a small full-text or vector hit set (the sparse hint path), and a keyword column read under a top-level filter that was pushed into the column scan.
- The shard path reader (`GET /_doc/{id}`, `_count`, stats, and the shapes listed under "Query shapes routed to the shard path") is a view over the same snapshot and reads columns from the same store. It stays open for the life of the shard at its served version, so it pins that snapshot: a shard that is not refreshed keeps one dataset open per node that hosts it, and a shard that falls through to a heap column load (store full) keeps that heap column until the next refresh swaps the reader.
- `_id` and `_source` are not served from whole-column loads on either path. The fragment path fetches the rows behind the hits with a `_rowaddr IN (...)` take per leaf, so a `size:10` fetch reads ten rows of the projected columns regardless of table size. Queries that never render hits (aggregations, `size=0` hit counts) only touch the columns they consult.
- When the column store is full and nothing can be evicted (every held column is being read by a running request), the request loads the column into heap for itself, with the query's filter applied as before the store existed. The store's budget is `lance.cache.column_share` of `lance.native_memory.limit`; on a multi-node cluster every data node holds the columns of the fragments it executes, so a node that executes a subset of the fragments holds that subset only.
- An inverted index has to fit one shard of the Lance index cache as a single entry, or every full-text query reloads it from storage (about 2.9 s at 100M rows). The entry is about 52 bytes per row per full-text column: about 4.8 GiB at 100M rows, about 48 GiB at 1B rows. The share per shard is the capacity the plugin chose divided by the shard count (`GET /_lance/stats`, `native_memory.index_cache_shard_share`); with the default settings on a 16 CPU node it is 8 GiB, so 100M rows fit and 1B rows are not expected to fit on one node under any setting (a share of 48 GiB needs a capacity of 8 times that on 16 CPUs). Whether the 1B row table's inverted index is retained is to be confirmed on the perf1b table once its index is built. `POST /_lance/attach` logs a warning when a table's estimate is heavier than the share.
- Snapshots retired by the namespace poll (table advanced, tag moved) are closed only on the elected cluster manager, where the poll runs. Other nodes keep the previous version's snapshot until `lance.cache.max_snapshots` evicts it or the index is deleted; requests on those nodes already key on the new version, so the leftover costs one open dataset and its columns, not stale results.

## Version pinning

- `"version": N` on `POST /_lance/attach` pins a Lance-backed index to a specific manifest version for readonly snapshots. Tag and branch checkout are not exposed by the Lance Java SDK (v11 today), so they cannot be pinned from OpenSearch.

## Snapshot and restore

- `_snapshot` of a Lance-backed index stores the index metadata (settings including `index.lance.table`, `index.lance.version`, `index.lance.storage_options.*`, and the mapping) plus the shard's empty Lucene commit: one `segments_N` file of about 200 bytes. Rows and the Lance-side FTS / vector indexes stay in the Lance table and are not copied, so a snapshot is not a backup of the data. Back up the table with the storage layer's own tooling.
- Restore re-creates the index with the same settings and mapping. If the table is reachable at `index.lance.table`, the shard opens and `_search`, `_count`, GET and `_stats` serve the table as it is at restore time. An index without `index.lance.version` reads the latest manifest version, so rows appended or deleted after the snapshot are visible; a pinned index keeps its pin.
- If the table is not reachable, the engine fails to open (`Dataset at path ... was not found`) and every allocation attempt up to `index.allocation.max_retries` reports that error in `_cluster/allocation/explain`. The index stays red with the primary `UNASSIGNED` / `ALLOCATION_FAILED`. `_search` and `_count` both return 400 with the Lance not-found message because the fragment path they run on cannot open the table either. Once the retries are exhausted the restore itself is marked failed, so `POST /_cluster/reroute?retry_failed=true` does not help even after the table is back: OpenSearch refuses to allocate a primary whose restore has failed. Delete the index and restore it again.
- The same engine failure outside a restore (for example `POST /{index}/_open` while the table is unreachable) behaves differently after the table is back: `POST /_cluster/reroute?retry_failed=true` starts the shard, because each failed attempt releases the shard's store and shard lock before the next one.

## Not yet implemented (RFC future work)

Items on the roadmap that no version of the plugin ships today.

- Analytics route via `sandbox/plugins/analytics-backend-datafusion`. Every query currently takes the reader route.
- PPL / SQL integration (lives in `opensearch-project/sql`).
- Text analysis beyond Lance's native tokenizer. The RFC's second text mode (OpenSearch analyzer plus a derived column backfilled by the plugin) is only proven for English today.
- Native ingestion via `_bulk` / `_doc` (RFC future work item 1).
- Lucene custom index type stored in `_indices/{uuid}/` (RFC future work item 2).

Full backlog: the issues tab of this repository.
