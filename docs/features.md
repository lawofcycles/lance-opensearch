# Features

Reference of what the plugin surfaces on a Lance-backed OpenSearch index. Grouped by concern so each subsection can be read on its own; start with [getting-started.md](getting-started.md) for a walkthrough that stitches these together.

Every Lance-backed index carries a single primary shard (attach rejects `number_of_shards`). `_search` runs through a fragment fan-out: the coordinator assigns Lance fragments round-robin to every data node in the cluster, each node's executor drives OpenSearch's stock aggregator machinery and Lucene `IndexSearcher` against per-fragment leaves, and the coordinator merges the per-node results (hits by the request's sort or by score, aggregations through the stock reduce).

Distribution over the cluster is automatic: fragments are spread over every data node that runs the plugin, and no replica setting is involved. A node does not need a shard copy of the index to execute its share. The executor builds its query context (mapping, `QueryShardContext`, the security plugin's reader wrapper) from the index metadata in cluster state; on the one node that hosts the shard it reuses the node's own `IndexService`, on every other node it creates a temporary `IndexService` for the duration of the request through the same `onIndexModule` hooks a regular index goes through. Lance fragments are read from external storage by whichever node gets them. Fragments are dealt round-robin by fragment id to the data nodes sorted by node id, each node returns its top `from + size` hits, and the coordinator merges them by sort value (or score) with ties broken by node id order, then applies `from` / `size`. `search_after` is applied by each node before the merge. A failure on any one node fails the request; there is no partial-result mode. A full-text lookup is run by every node against the whole table's inverted index (no fragment list is passed to Lance) and each node keeps the rows of its own fragments, so the per-node results are disjoint shares of the same global result and the lookup cost does not depend on the row count; see limitations.md for the probe limit that bounds this on shapes needing every match.

## Attach and namespace surface

- `POST /_lance/namespace` registers a directory as a Lance namespace. The plugin polls it for `*.lance` tables and surfaces each as an OpenSearch index. Mapping is derived from the Lance Arrow schema on every surface.
- `POST /_lance/attach` attaches a single Lance table URI directly. Idempotent: a repeated call for the same URI returns `already_attached: true`; a name clash with a non-Lance index or a Lance index for a different table returns 409. The body takes either `"version": N` (fixed pin, see below) or `"tag": "name"` (follow a Lance tag); both together return 400, and an unknown tag returns 400 with Lance's message. The request can be sent to any node: it is forwarded to the elected cluster manager, which opens the table, creates the index and records the index for the poll.
- `GET /_lance/refs/{index}` lists the tags (`name`, `version`) and branches (`name`) of the Lance table behind an index: `{"index": ..., "table": ..., "tags": [...], "branches": [...]}`. Unknown index returns 404, a non-Lance index 400.
- `POST /_lance/namespace/tables {"path": "..."}` returns the table names the poll would surface from a registered namespace. Read-only preview, useful for spotting a table the poll skipped due to a name clash. Unregistered paths return 404.
- `DELETE /_lance/namespace {"path": "..."}` stops polling that namespace. Already-surfaced indexes stay in place; delete them separately if the tables should disappear.
- `POST /_lance/build_indexes/{index}` triggers Lance-side FTS / scalar / vector index builds from OpenSearch. Automatic builds happen for tables at or under `lance.builder.max_rows` (default 1,000,000 rows); larger tables use this explicit endpoint. `fts_columns`, `tokenizer` and `with_position` create inverted indexes on Utf8 columns that have none yet (see [Full-text search](#full-text-search)).
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
- `cluster:monitor/lance/stats` for `GET /_lance/stats` (read-only roles; it reveals cache counters and byte totals, no table content).

## Query shapes

Full-text, vector, filter, and hit-shape queries all run on the fragment executor unless noted otherwise. Refer to [limitations.md](limitations.md) for shapes that fall through to the shard path.

### Full-text search

- Stock `match` and `bool` composition on a `lance_text` field, BM25 scored by Lance. The whole query string reaches Lance as one token; use the `lance_*` DSL below for token-level control.
- `lance_match` for AND / OR operator control, fuzziness, prefix length, and max term expansions (per-field, pushed straight into Lance's `MatchQuery`).
- `lance_match_phrase` for phrase order with optional `slop`. Needs an inverted index that stores token positions (Lance `with_position`); on an index built without them Lance rejects the query with `position is not found but required for phrase queries` and the search fails with that message. See [Building an FTS index](#building-an-fts-index-and-choosing-its-tokenizer) for `with_position` on `build_indexes`.
- `lance_multi_match` for multi-field FTS with per-field boosts and a shared operator (`Lance MultiMatchQuery`).
- `lance_fts_boost` combines a positive and a negative FTS clause with a `negative_boost` scale, evaluated on Lance rather than layered on Lucene's `BooleanQuery`.
- `lance_fts_bool` composes `must` / `should` / `must_not` FTS clause arrays on Lance's side.
- A `bool` whose `must` is a single `lance_*` FTS clause and whose `filter` / `must_not` clauses are `term`, `terms`, `range`, `exists`, `match_all`, or a `bool` of those on mapped columns runs as one Lance FTS scan with the scalar clauses as a SQL prefilter (`filter(sql)` + `prefilter(true)`), the same way `lance_knn`'s `filter` does. Lance evaluates the predicate first, through the column's scalar index when it has one, and looks up the inverted index only for the selected rows; `hits.total.value` comes from the same prefiltered scan. The bool must have no `should`, no `minimum_should_match`, and a boost of `1.0` (the FTS clause's own boost is kept); any other composition, a clause the translator cannot express (stock `match`, multi-field sub-fields, unmapped fields), or a security plugin reader wrapper (DLS / FLS) keeps the bool on the Lucene side with unchanged results.

#### Building an FTS index and choosing its tokenizer

- A Utf8 column is `lance_text` only when the Lance table already carries an inverted index on it; otherwise it is `keyword`, and a plain `POST /_lance/build_indexes/{index}` gives it a BTree scalar index. To create the inverted index from OpenSearch, name the column in `fts_columns`; the next namespace poll re-derives the mapping and rebuilds the index as `lance_text`:

  ```json
  POST /_lance/build_indexes/demo
  { "fts_columns": ["text"], "tokenizer": "lindera/ipadic", "with_position": true }
  ```

- `tokenizer` (default `simple`) is passed to Lance as the inverted index `base_tokenizer` without an allowlist; Lance 12 accepts `simple`, `whitespace`, `raw`, `ngram`, `icu`, `icu/split`, `lindera/<model>`, `jieba/<model>`. A name Lance rejects returns 400 with Lance's message under `failed.fts`. `tokenizer` requires `fts_columns`; neither is accepted with `optimize`.
- `with_position` (boolean, default `false`, the same as Lance's default) makes Lance store token positions in the inverted indexes this request creates. `lance_match_phrase` needs them; `lance_match`, `lance_multi_match`, `lance_fts_bool` and `lance_fts_boost` do not, and an index with positions is larger. Like `tokenizer`, it requires `fts_columns` and is not accepted with `optimize`. Whether positions are stored is fixed when the index is created, so a column built without them has to be dropped on the Lance side and built again to gain phrase support. A `lance_match_phrase` query on a column without positions answers 400 `illegal_argument_exception` with Lance's message (`position is not found but required for phrase queries ...`).
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
- Hits with equal scores or equal sort values are ordered by their Lance row address (`fragment id`, then offset) ascending, which is the doc id order of one Lucene reader over the whole table, so the order does not depend on how many nodes served the fragments; `_doc` sort means the same order. Each executor ships the row address of every hit to the coordinator over the transport layer only; it is not part of the response. Which tied rows a bare full-text page (no `sort`) contains at its cut is decided by Lance's bounded scan; see [limitations.md](limitations.md).
- `post_filter` narrows hits without affecting aggregations.
- `sort` by scalar field, and Painless `script` query / `script` sort.
- `track_scores: true` alongside `sort` keeps per-hit `_score` populated (Lucene's 4-argument `search(query, size, sort, doDocScores)` overload). `max_score` is the largest per-hit score in the paged window; NaN scores fall through so a sort-only query without `track_scores` reports `max_score: null` matching the shard path.
- `track_total_hits` follows the OpenSearch contract: omitted counts up to 10,000 and reports `{"value": 10000, "relation": "gte"}` beyond that, an integer sets that bound, `true` counts exactly, `false` omits `hits.total`. The coordinator ships the bound to every executor and composes the relation from the per-node counts; whenever the relation is `gte` the value is the bound itself, as on the shard path, also when the per-node counts of several nodes add up to less than the bound because each node counted its own fragments' share of a scan stopped at the bound. Scalar filters and `match_all` are counted from Lance metadata and are always exact on the executor; a Lance FTS query (`match`, `lance_*`, or a `bool` collapsed into a prefiltered FTS scan) is counted from the same scan that served the hits or aggregations when that scan saw every match, and otherwise by a count-only scan stopped at `bound + 1` rows, so a query with many hits costs the same as one with few until `track_total_hits: true` asks for the exact number.
- `_count` runs on the fragment path (it is a `_search` with `size: 0` and `track_total_hits: true`), so it reads the same manifest version and takes the same count paths as `_search`; for an FTS query that is one count-only Lance scan over the inverted index.

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
- Bucket: `terms`, `histogram`, `date_histogram`, `composite` over `terms` and `date_histogram` sources.
- All run through OpenSearch's standard aggregator machinery over Lance-backed doc values, except the shapes below, which the scan computes.
- Pipeline aggregations (`avg_bucket`, `bucket_sort`, `cumulative_sum`, etc.) fall through to the shard path (see [limitations.md](limitations.md)).

### Aggregation pushdown

- A `size: 0` request whose query is `match_all` or a scalar filter the coordinator translates to Lance SQL (`term`, `terms`, `range`, `exists`, `bool` of those) and whose aggregation tree is one of the following runs as a Substrait `AggregateRel` inside the Lance scan: each executor asks Lance for one row per group over its fragments, builds the same per node `InternalAggregation` the aggregators would have built, and the coordinator reduces them as before. `hits.total` comes from the same scan's `count(*)`.
  - Metric aggregations only (`sum`, `avg`, `min`, `max`, `value_count`), any number.
  - A chain of up to three bucket aggregations, each `terms` (`order` `_count` descending or `_key`, default `min_doc_count`, no `include` / `exclude`), `histogram` (`offset` 0, no `extended_bounds` / `hard_bounds`) or `date_histogram` (`fixed_interval`, or `calendar_interval` `second` / `minute` / `hour` / `day` / `week` / `month` / `quarter` / `year` on a timestamp column without a zone or in UTC; `offset` 0, no bounds, no `time_zone`), with metric children at every level and at most one nested bucket per level (`terms > terms > avg`, `terms > date_histogram`, `histogram > terms`). The scan groups by every key at once; the executor folds the rows into the tree, so an outer bucket's `doc_count` and metrics cover every row of its key, including the rows the nested level has no value for.
  - One `composite` whose sources are `terms` (either order) or `date_histogram` (a fixed length interval, `offset` 0, no `time_zone`) with `missing_bucket` false, with metric children. The executor sorts the key combinations in source order, drops the ones at or before `after`, and returns the first `size` with the last as `after_key`, as a shard does; the coordinator's reduce merges the per node pages.
  - Fields must be mapped and backed by a scalar column: `keyword` on `utf8` (a keyword sub-field resolves to its base column), the integer types, `float` / `double`, `boolean`, `date`. `terms` on a `list<utf8>` column, metrics on `keyword` other than `value_count`, `histogram` on `date` / `boolean` fields, `missing`, scripts and `value_type` take the aggregators.
- `terms` keeps the top `shard_size` groups per executor (default `size * 1.5 + 10`), with `sum_other_doc_count` and `doc_count_error_upper_bound` following the shard rules, so a three node answer carries the same error bound a three shard index would. A nested `terms` applies the same rules inside each parent bucket, and the rows of a parent bucket the truncation drops are dropped with it. `histogram` and `date_histogram` return every bucket; `min_doc_count: 0` filling stays with the coordinator's reduce. Rows with a null bucket key open no bucket at that level but count toward the enclosing bucket and `hits.total`.
- Nested `terms` levels multiply the key combinations the scan may return. The executor estimates them as the product of the levels' `shard_size` and sends the request through the aggregators when the estimate exceeds `lance.aggregation.pushdown_max_groups`.
- Requests with a full-text or `lance_knn` query, a `post_filter`, hits (`size > 0`), or a reader wrapper (security plugin DLS / FLS) stay on the aggregators.
- A plan Lance rejects fails the request instead of falling back, so a missing function or a schema mismatch surfaces as an error rather than as a slow answer.
- Each executor scans its fragments in up to `lance.aggregation.pushdown_parallelism` contiguous groups at once (Lance runs the aggregate of one scan on a single thread) and merges the group rows by key before building its buckets, so `shard_size`, `sum_other_doc_count` and the error bound keep the meaning of a single scan per node. The extra scans run on the node's `search` thread pool; when that pool has no free thread the request's own thread scans the remaining groups.
- Settings: `lance.aggregation.pushdown` (dynamic, default `true`; `false` sends every aggregation through the aggregators, for before / after comparisons); `lance.aggregation.pushdown_parallelism` (dynamic, 1 to 32, default half the CPUs the JVM sees, at least 1; `1` is a single scan per executor); `lance.aggregation.pushdown_max_groups` (node setting, default `1000000`; the bound on the estimated key combinations of a nested `terms` tree).

## Doc values for full-text and vector hits

- When the top-level query is a Lance full-text or `lance_knn` clause (alone, with a pushed-down `filter` / `must_not`, or as the only clause Lucene collects from), the sort and aggregation columns are fetched for the matched rows only, with the same `_rowaddr IN (...)` take that already serves `_source`. The fragment reader receives the per-fragment hit set from the query's Weight and reads numeric, boolean and keyword doc values for those rows instead of scanning the column for the whole fragment.
- The take is used while the hit set covers at most 0.25 percent of a fragment's rows (`LanceFragmentLeafReader.SPARSE_RATIO`); above that, or when no Lance clause drives the search, the fragment falls back to the full column load. The threshold is the ratio of the two per-row costs: a take costs about 8 µs per row and the sequential column scan about 20 ns per row, so above one row in four hundred the scan is cheaper. A column the node's off-heap column store already holds for the fragment (see "Fragment path snapshot and column cache") is read from the store whatever the size of the hit set, for numeric, boolean and keyword columns alike, because reading a held column costs no scan at all; a hit set below the ratio never starts a store load on its own. Numeric columns also fall back per fragment the moment a doc outside the hit set is requested (a `should` with another clause, a `should` beside a `filter`), so results do not depend on the query shape. Keyword columns build their ordinal dictionary from the hit rows only when the Weight has shown that nothing else is collected on that fragment. Under a reader wrapper installed by another plugin (the security plugin's document and field level security reader) sparse keyword dictionaries are disabled and keyword columns load fully, because such a wrapper can hide rows the Lance scan returned and a dictionary built from every hit row would expose their terms through its value count; numeric and boolean columns keep the per-hit take, since they are read only for the documents the wrapper lets through.
- When the top-level query is the bare Lance clause (alone, or with a pushed-down `filter` / `must_not`; a `post_filter` may be present) and the request carries aggregations or a sorted page, the fragment executor runs the clause's scan and delivers the per-fragment hit set to the readers before it builds the aggregators and sort comparators. A keyword `terms` aggregation (global ordinals are built when the aggregator is created, also with `size: 0`) and a keyword sort (the comparator looks the current bottom term up when the leaf comparator is created) therefore read the dictionary of the hit rows on every fragment. The same scan then serves the hits phase and the aggregations. A Lance clause nested in a `bool` with other Lucene clauses, or wrapped in `constant_score`, gets no early hint and follows the Weight-driven rules above.

## Follow-forward and version pinning

- The plugin follows the Lance manifest forward automatically. When Lance advances to a new version, the poller notices, `LanceReaderManager` swaps in a fresh reader, and the next `_search` sees the new fragments. No index close, no shard reallocation, no request downtime.
- Attach also accepts `"version": N` in the body to pin an index to a specific manifest version. Pinned indices are excluded from the poll cycle (they must never advance, by design).
- Attach accepts `"tag": "name"` as a moving pin: the index reads the version the tag points at, stored in `index.lance.tag`. Every poll cycle resolves the tag again and refreshes the reader when the tag has been moved on the Lance side (forwards or backwards). Branches can only be listed (`GET /_lance/refs/{index}`); the Lance Java SDK has no branch checkout.
- `_search` (fragment path) and `GET /_doc/{id}` (engine path) do not share a freshness view. Fragment path reads the latest version per query (through the snapshot cache below); engine path advances only with the poll cadence. Both read the same snapshot once they are on the same version. See [limitations.md](limitations.md).

## Snapshot and column cache

- Every fragment path request takes its view of the table from a node scoped cache (`LanceWarmCache`) keyed by `(index uuid, Lance version)`. A snapshot holds the open Lance dataset, the fragment list with each fragment's row count and live-row bitmap, the set of full-text indexed columns and the derived column schema. The first request against a version builds it; later requests against the same version open no dataset, ask Lance for no index description and run no schema pass. Their leaf readers are views over the snapshot.
- The shard engine's whole-table reader (behind `GET /_doc/{id}`, `_stats` and the `_search` shapes that fall through to the shard path) is a view over the same snapshot. Opening the shard leases the snapshot of the version it serves; a refresh that follows the table to a new version leases that version's snapshot and releases the previous one when its last searcher closes. A node therefore holds one open dataset, one fragment list and one set of store columns per table version, whichever path touched it first, and the first `_search` after a shard opened builds nothing. The engine never retires a snapshot; the namespace poll does, after it has refreshed the shard.
- The coordinator opens the table once per request to enumerate its fragments and ships the manifest version it observed (the pinned or tag version, or the latest one) with every per-node request, so every executor of one request reads the same version and keys its snapshot on it without any Lance call. An index that follows the latest manifest therefore sees an append on the next `_search` exactly as before; a new version gets its own snapshot. An executor that receives no version (none is sent by the coordinator in this release; the fallback exists for older callers) resolves the latest version itself, from the newest cached dataset's `Dataset.latestVersion()` or by opening the table.
- Numeric, boolean, date and float columns a request reads for the whole fragment (sort or aggregation without a usable full-text or vector hit set, `_source` rendering aside) are loaded once per `(snapshot, column, fragment)` into an off-heap column store and kept there: Arrow `BigIntVector` / `BitVector` buffers allocated from a child of the plugin's Arrow allocator, holding the same normalised `long` values the heap path held (sortable long for floats, epoch millis for dates). Doc values read the buffers directly. One Lance scan per column covers every fragment the request needs and does not hold yet; the query's filter is not applied to that scan, so the slice serves every later query.
- Keyword and keyword array columns (including the base column of a `keyword` multi-field) are kept in the same store: per `(snapshot, column, fragment)` a sorted, duplicate-free term dictionary (`VarCharVector`) plus one ordinal per row (`IntVector`; keyword arrays add a row offset vector over the flattened ordinals). The load scans the column once, interns the values in heap for the duration of the load, checks the resulting size against the budget, and writes the vectors; later requests read the ordinals and terms in place and rebuild nothing. When the dictionary does not fit (the budget is exhausted by columns running requests read, or `lance.cache.column_share` is 0), the request keeps the dictionary and ordinals that scan produced as its own heap columns, so the column is scanned once either way; a zero budget skips the store before the scan. `lookupOrd` copies the term into a scratch owned by the doc values instance. Two keyword loads stay on the request scoped heap path: the dictionary of a small full-text or vector hit set (next bullet), and a request whose top-level filter was pushed into the column scan (its ordinals cover the matching rows only, so the result cannot be shared).
- Order of the doc value sources on a fragment: the rows taken for a small full-text or vector hit set (see the section above), then the off-heap store, then a store load, then a request scoped heap load when the store has no room. Eviction is least recently used at `(column, fragment)` granularity and never touches a column a running request reads; a request that finds no room loads into heap for itself (`DEBUG` log `column cache budget exhausted`).
- Snapshots are closed when the namespace poll sees the table (or its tag) move to another version, when the index is deleted, when `lance.cache.enabled` is set to `false`, or when they are the least recently used ones above `lance.cache.max_snapshots`; a snapshot a request or a shard reader still holds closes when that holder ends. Closing a snapshot releases its columns, unless a newer snapshot of the same `(index uuid, version)` has taken its place (a shard reader can outlive a `lance.cache.enabled` off and on), in which case the columns stay with the replacement.
- Settings: `lance.cache.enabled` (dynamic, default `true`; `false` retires every snapshot, and from then on each request and each shard reader opens the table, runs the schema pass and loads its columns into heap for itself; concurrent requests against the same index do not share any of that, the same as before the cache existed), `lance.cache.max_snapshots` (static, default 64; only snapshots nobody holds are evicted, so shard readers never count against it), `lance.cache.column_share` (static, default 0.4: the fraction of `lance.native_memory.limit` the column store may use; see "Native memory bounds").

### Cache statistics

`GET /_lance/stats` (and `GET /_lance/stats/{nodeId}`, comma separated node ids) reports the cache per node, in the `_nodes` / `cluster_name` / `nodes` envelope of `_nodes/stats`. Read only; there is no API that clears the cache. The action name is `cluster:monitor/lance/stats`. Per node:

- `snapshots`: `enabled` (current `lance.cache.enabled`), `count` (snapshots held, referenced or idle), `retired` (held snapshots that were retired but are still leased, most often a shard's previous reader waiting for its last searcher), `dataset_open_count`, `snapshot_build_count`, `snapshot_hit_count` (acquires served by an existing snapshot). Counters are cumulative since node start.
- `column_store`: `bytes` (off-heap bytes the store holds), `limit_bytes` (its budget), `entries` (`(snapshot, column, fragment)` slices held), `hits` (column requests answered without a scan), `loads` (scans run to fill slices), `evictions`, `budget_misses` (requests that loaded into heap because the budget could not be met), `heap_fallback_bytes` (heap bytes those loads currently have charged to the request circuit breaker across the node's open readers; zero once every reader that charged has closed) and `heap_fallback_rejections` (heap loads the request breaker refused, each of which ended a request with HTTP 429).
- `native_memory`: `estimated_bytes` (what the `lance_native` breaker currently accounts for, as of the last sampler tick), `session_bytes` (`Session.sizeBytes()` of the shared Lance session, read live), `column_store_bytes` (the same value as `column_store.bytes`), `index_cache_capacity`, `index_cache_shards` and `index_cache_shard_share` (the index cache handed to Lance at startup, the shard count Lance derives from it and the heaviest entry one shard admits). The breaker estimate is the sum of `session_bytes` and `column_store_bytes` as sampled up to one `lance.native_memory.circuit_breaker.poll_interval` ago.
- `fts`: `subset_probe_limit` (current `lance.fts.subset_probe_limit`).

A per kind breakdown of `column_store.entries` (numeric, keyword, keyword array) is not reported; the store does not expose its entries by kind.

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

Lance's index cache and metadata cache are shared across every dataset opened on a node through a single Lance `Session`; the fragment path's off-heap column store shares the same budget. Bounds:

- **`lance.native_memory.limit`** — Absolute (`10gb`) or a percentage of host memory left after the JVM heap (`40%`, default). `lance.cache.column_share` (default 0.4) of it goes to the off-heap column store; the rest is split 6:1 between the index cache and metadata cache. The index cache receives the capacity within its 6/7 budget whose share per Lance cache shard is largest (Lance refuses an entry heavier than one shard's share, and the shard count grows with the capacity); the difference stays unused. Static, node scope; rolling restart to change. The resolved split, shard count and share are logged at startup.
- **`lance_native` circuit breaker** — Sampled every `lance.native_memory.circuit_breaker.poll_interval` seconds (dynamic). Reports `Session.sizeBytes()` plus the column store's allocated bytes. Rejects FTS / knn queries and column store loads with a 429 `CircuitBreakingException` once the reading catches up to the limit. `lance.native_memory.circuit_breaker.enabled` toggles it (dynamic).
- **`lance.fragment_dispatch.max_concurrent`** — Semaphore cap on fragment path concurrency (default 4). Fragment path serves every fragment on one node, so per-query heap (the sparse FTS and knn hit lists of 8 bytes per matching row, aggregation buffers) scales with concurrency rather than shard fan-out. Static, node scope.
- **`request` circuit breaker (heap side)** — The Lance scan behind a full-text or knn query returns `_rowaddr` and `_score` (`_distance` for knn) only, so its native buffers hold about 12 bytes per matching row whatever columns the table has. The hits are then buffered on heap for the life of the request (8 bytes per matching row of the node's fragments, plus 8 bytes per row of each fragment's sorted view) and every buffer is reserved with OpenSearch's standard `request` breaker under the label `lance_fts_hits` before it is allocated, then returned when the request ends. A hit set the breaker refuses (an unbounded shape such as a sort by a field or an aggregation over a query that matches a large share of a large table) answers 429 `circuit_breaking_exception` instead of exhausting the heap; `indices.breaker.request.limit` bounds it.

Cache lifecycle: deleting a Lance-backed OpenSearch index (via `DELETE /<index>`) reinstalls the shared `Session` on the node the DELETE lands on. This drops every cached index / metadata page held by the Session so that a later re-attach against the same table URI cannot pick up stale `_indices/<uuid>/` pointers left behind by the deleted table. Every other Lance-backed index on that node pays a cold cache cost on its next query; the trade-off is unavoidable today because Lance's Java SDK does not expose a per-path invalidation hook. `CLOSED`, `NO_LONGER_ASSIGNED`, and other non-delete removal reasons keep the cache intact because they leave the underlying files in place.

See [limitations.md](limitations.md) for the guardrail rationale.
