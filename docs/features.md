# Features

Reference of what the plugin surfaces on a Lance-backed OpenSearch index. Grouped by concern so each subsection can be read on its own; start with [getting-started.md](getting-started.md) for a walkthrough that stitches these together.

Every Lance-backed index carries a single primary shard (attach rejects `number_of_shards`). Multi-node parallelism comes from replica placement (`index.number_of_replicas: data_nodes - 1`, or `auto_expand_replicas: 0-all`) so every data node holds a copy of the primary shard. `_search` runs through a fragment fan-out: the coordinator dispatches Lance fragments to the per-node executor holding the primary, and the executor drives OpenSearch's stock aggregator machinery and Lucene `IndexSearcher` against per-fragment leaves.

## Attach and namespace surface

- `POST /_lance/namespace` registers a directory as a Lance namespace. The plugin polls it for `*.lance` tables and surfaces each as an OpenSearch index. Mapping is derived from the Lance Arrow schema on every surface.
- `POST /_lance/attach` attaches a single Lance table URI directly. Idempotent: a repeated call for the same URI returns `already_attached: true`; a name clash with a non-Lance index or a Lance index for a different table returns 409.
- `POST /_lance/namespace/tables {"path": "..."}` returns the table names the poll would surface from a registered namespace. Read-only preview, useful for spotting a table the poll skipped due to a name clash. Unregistered paths return 404.
- `DELETE /_lance/namespace {"path": "..."}` stops polling that namespace. Already-surfaced indexes stay in place; delete them separately if the tables should disappear.
- `POST /_lance/build_indexes/{index}` triggers Lance-side FTS / scalar / vector index builds from OpenSearch. Automatic builds happen for tables at or under `lance.builder.max_rows` (default 1,000,000 rows); larger tables use this explicit endpoint.
- Poll cadence is controlled by the cluster setting `lance.namespace.poll_cadence` (default 10s).
- Resurface guard: when an operator runs `DELETE /{index}` on a Lance-backed index, the poll cycle honours that deletion for `lance.namespace.resurface_guard_grace` (default 1 hour, node-scoped dynamic). Once the grace expires the poll recreates the index if the underlying Lance table is still there. Set to `0` to disable the guard entirely.

### Authorization

Every `/_lance/*` endpoint runs through a transport action, so a security plugin evaluates the caller before the plugin opens a table, probes a path, or lists anything. Grant these action names to roles:

- `cluster:admin/lance/attach` for `POST /_lance/attach` (operator roles that may create Lance-backed indexes; the internal create-index call runs as the plugin, so `indices:admin/create` is not needed in addition).
- `cluster:admin/lance/namespace/update` for `POST` / `DELETE /_lance/namespace` (the same operator roles).
- `indices:admin/lance/build_indexes` as an index-level permission for `POST /_lance/build_indexes/{index}` (roles that own the Lance table behind that index; the build writes into the table). The refresh that follows the build runs as the caller, so the role also needs `indices:admin/refresh` on the index.
- `cluster:monitor/lance/namespace` for `GET /_lance/namespace` and `POST /_lance/namespace/tables` (read-only roles; it reveals registered paths and table names).

## Query shapes

Full-text, vector, filter, and hit-shape queries all run on the fragment executor unless noted otherwise. Refer to [limitations.md](limitations.md) for shapes that fall through to the shard path.

### Full-text search

- Stock `match` and `bool` composition on a `lance_text` field, BM25 scored by Lance. The whole query string reaches Lance as one token; use the `lance_*` DSL below for token-level control.
- `lance_match` for AND / OR operator control, fuzziness, prefix length, and max term expansions (per-field, pushed straight into Lance's `MatchQuery`).
- `lance_match_phrase` for phrase order with optional `slop`.
- `lance_multi_match` for multi-field FTS with per-field boosts and a shared operator (`Lance MultiMatchQuery`).
- `lance_fts_boost` combines a positive and a negative FTS clause with a `negative_boost` scale, evaluated on Lance rather than layered on Lucene's `BooleanQuery`.
- `lance_fts_bool` composes `must` / `should` / `must_not` FTS clause arrays on Lance's side.

### Vector nearest neighbour

- `lance_knn` on a `knn_vector` (Lance `fixed_size_list<float32>`) column. Per-fragment nearest scan; the coordinator merge reconstructs the global top-K.
- Optional inner `filter` clause is evaluated by Lance before the K-nearest cutoff (pre-filter): supported clauses are `match_all`, `term`, `terms`, `exists`, `range`, and `bool`.
- Score is `boost / (1 + distance)`; comparable within a query, not across queries or engines.

### GET by primary key

- `GET /<index>/_doc/<id>` maps to a Lance filter (`<field> = <literal>`) restricted to the shard's fragments. When the PK column carries a Lance scalar index the lookup is log time; without one it is a filtered scan.
- PK column type is inferred at attach time from the Lance `lance-schema:unenforced-primary-key` metadata:
  - **Signed integer** (up to 64 bits): id is parsed through `Long.parseLong`; the filter is `<field> = <long>`.
  - **Utf8**: id is placed inside single quotes; single quotes inside the id are doubled to prevent injection. Empty id short-circuits to 404.
  - **UInt64**: id is parsed through `BigInteger`; the filter uses the wide decimal literal Lance's SQL accepts. The PK column also surfaces as an OpenSearch `unsigned_long` mapping, so term / range / sort / aggregation resolve through the built-in unsigned semantics. Non-PK UInt64 columns are not surfaced today.
- Tables without a declared PK expose an empty `primary_key_field`; GET returns 404, and `_id` on `_search` hits is synthesised as `<fragment>-<offset>` so sort-by-`_id` and `_mget` dedup stay correct.
- **PK metadata placement**: attach only honours the `lance-schema:unenforced-primary-key` marker when it is attached as **field-level** metadata on the PK column itself (`FieldType(nullable=false, ..., {"lance-schema:unenforced-primary-key": "true"})` in Arrow terms). Placing the same key on the Arrow schema-level metadata is silently ignored by both the Lance reader and the attach derivation, so a writer that puts the marker there ends up with a PK-less attach and no obvious reason why `GET /_doc/{id}` returns 404. Use field-level metadata.
- **PK column nullability**: Lance's schema validator refuses a nullable primary key column ("Primary key column and all its ancestors must not be nullable"). `Dataset.create` on Lance 11 rejects the shape at write time, so the current Lance SDK cannot even build such a table. Attach adds a matching 400 response in case an older-format or hand-crafted table with a nullable PK column still reaches the derivation, so the error surfaces at attach time with the column name rather than as a 500 during shard recovery.

### Hit shape

- `_source` and `_id` synthesised on the fly from Lance rows.
- `from` + `size` pagination.
- `search_after` cursor pagination when the request carries a `sort` clause.
- `post_filter` narrows hits without affecting aggregations.
- `sort` by scalar field, and Painless `script` query / `script` sort.
- `track_scores: true` alongside `sort` keeps per-hit `_score` populated (Lucene's 4-argument `search(query, size, sort, doDocScores)` overload). `max_score` is the largest per-hit score in the paged window; NaN scores fall through so a sort-only query without `track_scores` reports `max_score: null` matching the shard path.

### Multi-fields

- Attach body accepts a `multi_fields` clause (or the equivalent `overrides.[col].fields` clause, see below) that declares a `keyword` sub-field on a Utf8 base column, so a single Lance column serves both full-text (`lance_text`) and exact-match / aggregation (`.raw`) without duplicating source:

  ```json
  POST /_lance/attach
  {
    "table": "s3://bucket/tables/demo.lance",
    "overrides": { "body": { "fields": { "raw": { "type": "keyword" } } } }
  }
  ```

  The legacy shape `"multi_fields": {"body": {"raw": {"type": "keyword"}}}` is still accepted for backward compatibility; both shapes end up in the same `index.lance.multi_fields` setting.

- The base column must be Utf8 (`lance_text` or `keyword`); other Arrow types are rejected at attach with a 400. Sub-field type must be `keyword` today. Base column type overrides (`overrides.[col].type`) are reserved for `ip` / `wildcard`, analyzer mode, and preferred index type; today the parser refuses them with 400.
- Persisted in `index.lance.multi_fields` (an index setting). Namespace poll re-derivation reads the setting back and re-applies it on every manifest version advance, so multi-field declarations survive schema changes.

## Aggregations

- Metric: `sum`, `avg`, `min`, `max`, `value_count`.
- Bucket: `terms`, `histogram`, `date_histogram`.
- All run through OpenSearch's standard aggregator machinery over Lance-backed doc values.
- Pipeline aggregations (`avg_bucket`, `bucket_sort`, `cumulative_sum`, etc.) fall through to the shard path (see [limitations.md](limitations.md)).

## Follow-forward and version pinning

- The plugin follows the Lance manifest forward automatically. When Lance advances to a new version, the poller notices, `LanceReaderManager` swaps in a fresh reader, and the next `_search` sees the new fragments. No index close, no shard reallocation, no request downtime.
- Attach also accepts `"version": N` in the body to pin an index to a specific manifest version. Pinned indices are excluded from the poll cycle (they must never advance, by design). Tag and branch checkout are not yet exposed by the Lance Java SDK.
- `_search` (fragment path) and `GET /_doc/{id}` (engine path) do not share a freshness view. Fragment path re-opens the latest dataset per query; engine path advances only with the poll cadence. See [limitations.md](limitations.md).

## Hybrid search

- Full-text and vector sub-queries compose inside compound queries at the shard level. `bool.should` with an FTS clause (`lance_match`, stock `match` on `lance_text`, `lance_match_phrase`, `lance_multi_match`) and a `lance_knn` clause returns the union of hits; the sum-of-child-scores ranks docs satisfying both sub-queries at the top. ITs `testBoolShouldComposesLanceMatchWithLanceKnn` and `testBoolShouldComposesStockMatchOnLanceTextWithLanceKnn` cover this end to end.
- OpenSearch's dedicated `hybrid` query (from the `neural-search` plugin, with its `normalization-processor` / `combination-processor` search pipeline) also composes on top of the Lance queries here because they implement the standard Lucene `Query` / `Weight` / `ScorerSupplier` / `Scorer` contract. Install `neural-search` alongside this plugin to use it. An end-to-end IT with `neural-search` installed is tracked separately.

## Storage options

- Per-table object-store credentials, endpoints, and timeouts are supplied through a `storage_options` map on `POST /_lance/attach` and `POST /_lance/namespace`:

  ```json
  "storage_options": {
    "aws_access_key_id": "...",
    "aws_secret_access_key": "...",
    "aws_region": "us-west-2",
    "aws_endpoint": "https://s3.example.com",
    "allow_http": "true"
  }
  ```

- Keys follow Lance's Rust `object_store` naming. Values ride into `index.lance.storage_options.<key>` settings so the same JVM can address multiple buckets with different credentials concurrently.
- Empty map falls back to Lance's env-var lookup (`AWS_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `AWS_ALLOW_HTTP`).

## Mapping type coverage

Types listed here map to real OpenSearch field types with doc values or FTS backing.

| Arrow type | OpenSearch mapping | Notes |
|---|---|---|
| `int8` / `int16` / `int32` / `int64` (signed) | `byte` / `short` / `integer` / `long` | Unsigned integer variants are noted in the attach response and left unmapped, except for UInt64 declared as the primary key (see next row). |
| `uint64` declared as the primary key | `unsigned_long` | Values are held as raw 64-bit patterns; `Long.toUnsignedString` decodes the `_id`, and `BigInteger` handles the `GET /_doc/{id}` parse. Non-PK UInt64 columns are still not surfaced. |
| `float32` / `float64` (scalar) | `float` / `double` | Values are folded into the shared `long[]` doc value storage via `NumericUtils.floatToSortableInt` / `doubleToSortableLong` and decoded on the way out so range, sort, metric aggregation, and `_source` all round trip. `float16` stays unmapped. |
| `boolean` | `boolean` | |
| `date` / `timestamp` (all units and TZs) | `date` | Normalised to epoch millis in the reader. |
| `utf8` with a Lance FTS index | `lance_text` | Enables `match`, `lance_match`, `lance_match_phrase`, `lance_multi_match`. |
| `utf8` without a Lance FTS index | `keyword` | SortedSetDocValues, so `term` / `terms` / `terms` aggregation work. |
| `list<utf8>` | multi-valued `keyword` | |
| `fixed_size_list<float32>` | `knn_vector` | Dimension carried through the mapping; `lance_knn` validates it. |
| `binary` / `large_binary` | `binary` | Base64 in `_source`, no doc values. |

Multi-fields (`fields.raw: keyword` on a Utf8 base column) is supported through the `multi_fields` attach clause above.

Types not yet surfaced: `ip`, `wildcard`, `object` (Arrow `Struct`), `nested` (Arrow `List<Struct>`), and the geo family. `Utf8` list, `Decimal`, and `FloatingPoint(HALF)` are stored in the table but excluded from the mapping today; the attach response notes them.

## Native memory bounds

Lance's index cache and metadata cache are shared across every dataset opened on a node through a single Lance `Session`. Bounds:

- **`lance.native_memory.limit`** — Absolute (`10gb`) or a percentage of host memory left after the JVM heap (`40%`, default). Split 6:1 between the index cache and metadata cache. Static, node scope; rolling restart to change.
- **`lance_native` circuit breaker** — Sampled every `lance.native_memory.circuit_breaker.poll_interval` seconds (dynamic). Rejects FTS / knn queries with a 429 `CircuitBreakingException` once `Session.sizeBytes()` catches up to the limit. `lance.native_memory.circuit_breaker.enabled` toggles it (dynamic).
- **`lance.fragment_dispatch.max_concurrent`** — Semaphore cap on fragment path concurrency (default 4). Fragment path serves every fragment on one node, so per-query heap (FTS score arrays sized by `maxDoc`, aggregation buffers) scales with concurrency rather than shard fan-out. Static, node scope.

Cache lifecycle: deleting a Lance-backed OpenSearch index (via `DELETE /<index>`) reinstalls the shared `Session` on the node the DELETE lands on. This drops every cached index / metadata page held by the Session so that a later re-attach against the same table URI cannot pick up stale `_indices/<uuid>/` pointers left behind by the deleted table. Every other Lance-backed index on that node pays a cold cache cost on its next query; the trade-off is unavoidable today because Lance's Java SDK does not expose a per-path invalidation hook. `CLOSED`, `NO_LONGER_ASSIGNED`, and other non-delete removal reasons keep the cache intact because they leave the underlying files in place.

See [limitations.md](limitations.md) for the guardrail rationale.
