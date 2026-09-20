# Features

Reference of what the plugin surfaces on a Lance-backed OpenSearch index. Grouped by concern so each subsection can be read on its own; start with [getting-started.md](getting-started.md) for a walkthrough that stitches these together.

Every Lance-backed index carries a single primary shard (attach rejects `number_of_shards`). `_search` runs through a fragment fan-out: the coordinator assigns Lance fragments round-robin to every data node in the cluster, each node's executor drives OpenSearch's stock aggregator machinery and Lucene `IndexSearcher` against per-fragment leaves, and the coordinator merges the per-node results (hits by the request's sort or by score, aggregations through the stock reduce).

Distribution over the cluster is automatic: fragments are spread over every data node that runs the plugin, and no replica setting is involved. A node does not need a shard copy of the index to execute its share. The executor builds its query context (mapping, `QueryShardContext`, the security plugin's reader wrapper) from the index metadata in cluster state; on the one node that hosts the shard it reuses the node's own `IndexService`, on every other node it creates a temporary `IndexService` for the duration of the request through the same `onIndexModule` hooks a regular index goes through. Lance fragments are read from external storage by whichever node gets them. Fragments are dealt round-robin by fragment id to the data nodes sorted by node id, each node returns its top `from + size` hits, and the coordinator merges them by sort value (or score) with ties broken by node id order, then applies `from` / `size`. `search_after` is applied by each node before the merge. A failure on any one node fails the request; there is no partial-result mode.

## Attach and namespace surface

- `POST /_lance/namespace` registers a directory as a Lance namespace. The plugin polls it for `*.lance` tables and surfaces each as an OpenSearch index. Mapping is derived from the Lance Arrow schema on every surface.
- `POST /_lance/attach` attaches a single Lance table URI directly. Idempotent: a repeated call for the same URI returns `already_attached: true`; a name clash with a non-Lance index or a Lance index for a different table returns 409. The body takes either `"version": N` (fixed pin, see below) or `"tag": "name"` (follow a Lance tag); both together return 400, and an unknown tag returns 400 with Lance's message.
- `GET /_lance/refs/{index}` lists the tags (`name`, `version`) and branches (`name`) of the Lance table behind an index: `{"index": ..., "table": ..., "tags": [...], "branches": [...]}`. Unknown index returns 404, a non-Lance index 400.
- `POST /_lance/namespace/tables {"path": "..."}` returns the table names the poll would surface from a registered namespace. Read-only preview, useful for spotting a table the poll skipped due to a name clash. Unregistered paths return 404.
- `DELETE /_lance/namespace {"path": "..."}` stops polling that namespace. Already-surfaced indexes stay in place; delete them separately if the tables should disappear.
- `POST /_lance/build_indexes/{index}` triggers Lance-side FTS / scalar / vector index builds from OpenSearch. Automatic builds happen for tables at or under `lance.builder.max_rows` (default 1,000,000 rows); larger tables use this explicit endpoint. `fts_columns` and `tokenizer` create inverted indexes on Utf8 columns that have none yet (see [Full-text search](#full-text-search)).
  - The response reports every target column under one of three keys, each split by index kind: `built` (`{"fts": [...], "scalar": [...], "vector": [...]}`, column names, or Lance index names with `optimize: true`), `skipped` (`{"fts": [{"column": "...", "reason": "..."}], ...}`, an index already exists, the table has fewer than 256 rows for IVF_PQ, or with `optimize: true` the column has no index to extend) and `failed` (same shape, `reason` is the message Lance threw, for example `LanceError(IO): Permission denied (os error 13)` when the OpenSearch process cannot write into the table).
  - Status: 200 when `failed` is empty (skips are not failures), 400 when every failure is Lance rejecting the input (unknown `tokenizer`, malformed index parameters), 500 for anything else Lance threw. The body carries `built`, `skipped` and `failed` in all three cases, so a build that landed on some columns and failed on others shows both. A build that stops before Lance runs (unknown index 404, a `columns` entry that is not indexable 400, `fts_columns` naming a non-Utf8 column 400) returns the usual error body instead.
- Poll cadence is controlled by the cluster setting `lance.namespace.poll_cadence` (default 10s).
- The poll's tracking of which index follows which table lives in the cluster manager's memory. Every poll cycle therefore scans cluster state and takes back any index that carries `index.lance.table` but is not tracked yet: an index restored from a snapshot, every Lance-backed index after a full cluster restart, and every Lance-backed index on a newly elected cluster manager. The table path, `index.lance.tag` and `index.lance.storage_options.*` come from the index settings, so nothing needs to be re-registered or re-attached. An index pinned with `index.lance.version` is left out, as it never advances. The table is opened once before the index is taken back; if that fails the poll logs one warning and tries again on the next cycle. The first cycle after adoption re-derives the mapping and refreshes the shard reader once, so `_count`, `_stats` and GET catch up with the table.
- Resurface guard: when an operator runs `DELETE /{index}` on a Lance-backed index, the poll cycle honours that deletion for `lance.namespace.resurface_guard_grace` (default 1 hour, node-scoped dynamic). Once the grace expires the poll recreates the index if the underlying Lance table is still there. Set to `0` to disable the guard entirely.

### Authorization

Every `/_lance/*` endpoint runs through a transport action, so a security plugin evaluates the caller before the plugin opens a table, probes a path, or lists anything. Grant these action names to roles:

- `cluster:admin/lance/attach` for `POST /_lance/attach` (operator roles that may create Lance-backed indexes). The internal create-index call runs under a stashed thread context with the plugin's internal header, so the role is expected not to need `indices:admin/create` in addition; this is still to be confirmed with the security plugin installed.
- `cluster:admin/lance/namespace/update` for `POST` / `DELETE /_lance/namespace` (the same operator roles).
- `indices:admin/lance/build_indexes` as an index-level permission for `POST /_lance/build_indexes/{index}` (roles that own the Lance table behind that index; the build writes into the table). The refresh that follows the build runs as the caller, so the role also needs `indices:admin/refresh` on the index.
- `indices:monitor/lance/refs` as an index-level permission for `GET /_lance/refs/{index}` (read-only roles; it reveals tag and branch names).
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
- A `bool` whose `must` is a single `lance_*` FTS clause and whose `filter` / `must_not` clauses are `term`, `terms`, `range`, `exists`, `match_all`, or a `bool` of those on mapped columns runs as one Lance FTS scan with the scalar clauses as a SQL prefilter (`filter(sql)` + `prefilter(true)`), the same way `lance_knn`'s `filter` does. Lance evaluates the predicate first, through the column's scalar index when it has one, and looks up the inverted index only for the selected rows; `hits.total.value` comes from the same prefiltered scan. The bool must have no `should`, no `minimum_should_match`, and a boost of `1.0` (the FTS clause's own boost is kept); any other composition, a clause the translator cannot express (stock `match`, multi-field sub-fields, unmapped fields), or a security plugin reader wrapper (DLS / FLS) keeps the bool on the Lucene side with unchanged results.

#### Building an FTS index and choosing its tokenizer

- A Utf8 column is `lance_text` only when the Lance table already carries an inverted index on it; otherwise it is `keyword`, and a plain `POST /_lance/build_indexes/{index}` gives it a BTree scalar index. To create the inverted index from OpenSearch, name the column in `fts_columns`; the next namespace poll re-derives the mapping and rebuilds the index as `lance_text`:

  ```json
  POST /_lance/build_indexes/demo
  { "fts_columns": ["text"], "tokenizer": "lindera/ipadic" }
  ```

- `tokenizer` (default `simple`) is passed to Lance as the inverted index `base_tokenizer` without an allowlist; Lance 12 accepts `simple`, `whitespace`, `raw`, `ngram`, `icu`, `icu/split`, `lindera/<model>`, `jieba/<model>`. A name Lance rejects returns 400 with Lance's message under `failed.fts`. `tokenizer` requires `fts_columns`; neither is accepted with `optimize`.
- The tokenizer is fixed when the index is created. A later `build_indexes` naming a column that already has an FTS index skips it (`"fts": []` under `built`, the column and reason under `skipped.fts`) and the existing index keeps its tokenizer; to switch, drop or replace the index on the Lance side (`Dataset.drop_index` / `create_scalar_index(..., replace=True)`) and build again.
- Japanese and Chinese text has no word separators, so `simple` indexes a whole sentence as one token and word queries miss. `icu` (ICU dictionary segmentation, compiled into the Lance native library) works with no setup. `lindera/ipadic`, `lindera/unidic`, `jieba/default` need a dictionary under `$LANCE_LANGUAGE_MODEL_HOME` (default `~/.local/share/lance/language_models` on Linux) on every data node, readable by the OpenSearch process: for Lindera, download `lindera-ipadic-<version>.zip` from the [lindera releases](https://github.com/lindera/lindera/releases), unpack it to `$LANCE_LANGUAGE_MODEL_HOME/lindera/ipadic/main`, and write `$LANCE_LANGUAGE_MODEL_HOME/lindera/ipadic/config.yml` containing `segmenter: {mode: normal, dictionary: <absolute path to main>}`; for Jieba, place jieba-rs's `dict.txt` at `$LANCE_LANGUAGE_MODEL_HOME/jieba/default/dict.txt`. Lance loads the dictionary when it builds the index and again whenever it opens the index for a query, so the files must stay in place on every node that serves the index.

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
- Attach also accepts `"version": N` in the body to pin an index to a specific manifest version. Pinned indices are excluded from the poll cycle (they must never advance, by design).
- Attach accepts `"tag": "name"` as a moving pin: the index reads the version the tag points at, stored in `index.lance.tag`. Every poll cycle resolves the tag again and refreshes the reader when the tag has been moved on the Lance side (forwards or backwards). Branches can only be listed (`GET /_lance/refs/{index}`); the Lance Java SDK has no branch checkout.
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
