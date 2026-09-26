# Mapping overrides and index type selection

The attach body and the namespace register body take two optional clauses next to the table: `overrides`, per column mapping rules that change the OpenSearch type the plugin derives from the Arrow schema, and `indexes`, the Lance index type the build creates per column. Both persist in the `index.lance.overrides` index setting and are re-applied on every manifest version advance. [features.md](features.md#mapping-type-coverage) lists the types the derivation picks without an override.

- [Mapping overrides](#mapping-overrides)
  - [`type: date`](#type-date)
  - [`type: keyword`](#type-keyword)
  - [`type: ip`](#type-ip)
  - [`type: wildcard`](#type-wildcard)
  - [`type: geo_point`](#type-geo_point)
  - [`type: text_analyzer`](#type-text_analyzer)
  - [`fields`](#fields)
  - [Validation](#validation)
  - [Persistence](#persistence)
- [Index type selection](#index-type-selection)

## Mapping overrides

The attach body accepts an `overrides` clause with per-column mapping rules. Seven kinds are supported: a `date` type override, a `keyword` type override, an `ip` type override, a `wildcard` type override, a `geo_point` type override, a `text_analyzer` type override, and keyword sub-fields (`fields`). `type` and `fields` may appear together on one column:

```json
POST /_lance/attach
{
  "table": "s3://bucket/tables/demo.lance",
  "overrides": {
    "ts": { "type": "date", "format": "epoch_millis" },
    "body": { "type": "keyword", "fields": { "raw": { "type": "keyword" } } },
    "client_addr": { "type": "ip" },
    "location": { "type": "geo_point" }
  }
}
```

### `type: date`

`type: date` on a signed 32 or 64 bit integer column reads the stored value as epoch millis. The mapping becomes `date` with the declared `format` (default `epoch_millis`), so range queries with ISO dates, `date_histogram`, and sort behave as on a real date column.

- An Int32 column is accepted and its values are read as millis too, which places them near 1970; useful only when that is what the writer stored.
- On a Date / Timestamp column the override is a no-op pin of the type the derivation picks anyway, so one override list can be applied uniformly to several tables.
- `_source` renders the raw integer, not a formatted date.

### `type: keyword`

`type: keyword` on a Utf8 column maps it to `keyword` even when the column carries a Lance inverted index, for operators that prefer exact matches and terms aggregations over `lance_text`.

- The column leaves the FTS build targets, so `build_indexes` neither creates nor optimises an FTS index for it; an inverted index the table already has stays untouched on the Lance side.
- On a List&lt;Utf8&gt; column the override pins the derived type.
- `lance_match` and friends on such a column are refused like on any other keyword field.

### `type: ip`

`type: ip` on a Utf8 column of IP address strings (or a List&lt;Utf8&gt; for the multi-valued shape) maps it to `ip`. The fragment reader parses each string and serves the 16 byte `InetAddressPoint` encoding through doc values, so `term` (exact or CIDR notation like `10.0.0.0/8`), `terms`, `range`, `exists`, sort and `terms` aggregations behave as on a stock `ip` field.

- Sort order and aggregation keys follow address order rather than string order; an IPv4-mapped IPv6 form like `::ffff:10.0.0.2` matches and buckets as `10.0.0.2`.
- `_source` renders the original strings.
- A string that does not parse as an IP address is served as missing (absent from `exists` and every bucket, present in `_source`); the count of such values is logged once per fragment.
- A keyword sub-field (`fields`) on the ip column serves the raw strings for exact string match.
- Predicates on an ip field never push down to Lance SQL: a range over the encoded form is not a lexical string range, and even a term equality only matches when the stored strings are canonical, which the plugin cannot know. The scan runs unfiltered and Lucene evaluates the predicate over the encoded doc values.

### `type: wildcard`

`type: wildcard` on a Utf8 column declares that the operator wants wildcard-style access to the column. On this plugin it is served by the keyword doc values path: the emitted mapping is `keyword` with `meta.lance_override_type: wildcard` recording the declared type, and `wildcard`, `prefix`, `regexp` and `term` queries evaluate against SortedSetDocValues per hit, exactly as on a `keyword` column, including the Lance SQL pushdown of those patterns to `LIKE` / `regexp_like` / `starts_with` over the stored Utf8.

- The n-gram-accelerated `wildcard` field type of OpenSearch core is not used because the fragment reader has no postings: in the 3.7 tree, `WildcardFieldType.wildcardQuery` always builds its first phase from `matchAllTermsQuery` over the pattern's required trigram terms with `isSearchable` hardcoded, so without an inverted index every query on that field type would match nothing.
- Like the `keyword` override, the column leaves the FTS build and optimise targets.

### `type: geo_point`

`type: geo_point` opts a column into OpenSearch's stock `geo_point` mapping. Two Lance shapes are accepted: a `Struct` with exactly two `Float64` children named as one of `(lat, lon)`, `(latitude, longitude)` or `(y, x)` in either order, where the child names fix the storage order and the operator declares no `order`; or a `FixedSizeList<Float64>[2]`, where `overrides.<col>.order` selects `lat_lon` (default) or `lon_lat`.

- The derived mapping is `{ "type": "geo_point" }` with `meta.lance_arrow_type` set to `struct` or `fsl2f64` and `meta.lance_geo_order` recording the resolved order.
- Struct child columns are not surfaced separately: the geo mapping replaces the object mapping the derivation would emit for the Struct, so the column appears once in `_mapping` and once in `_source` (rendered canonically as `{"lat": .., "lon": ..}`).
- The fragment reader loads the column in one Lance scan, encodes each pair into the `LatLonDocValuesField` long layout, and serves it two ways: as `SortedNumericDocValues` (what `exists`, `_geo_distance` sort and the geo aggregations read) and as a flat, single-cell Lucene point tree over the same array (what `geo_distance` / `geo_bounding_box` / `geo_polygon` / `geo_shape` intersect).
  - Why both: the stock query wraps its BKD query and its doc-values query in an `IndexOrDocValuesQuery` that matches nothing when either side is missing.
  - The flat tree has no BKD splitting, so a geo query visits every present row of a fragment once per query; correct, and proportional to fragment size rather than to the matching subset.
- A row whose cell or either component is Arrow null, or whose coordinates fall outside +/-90 / +/-180, is served as missing while staying readable in `_source`.
- Predicates on a `geo_point` field never push down to Lance SQL (DataFusion cannot address a struct child or a list element in a filter), so the scan runs unfiltered and Lucene evaluates the predicate.
- The geo aggregations (`geo_distance`, `geohash_grid`, `geotile_grid`, `geo_centroid`, `geo_bounds`) run on the fragment executors through the stock aggregators over the same doc values.
- Multi-valued geo points are not supported (the Arrow schema encodes one point per row).

### `type: text_analyzer`

`type: text_analyzer` on a Utf8 column selects the OpenSearch analyzer mode (the RFC's second text mode): the attach adds a derived tokens column to the Lance table, backfills it with the column's values run through the declared `analyzer`, builds an inverted index over it, and every full text query on the base field analyzes its text the same way and runs against the derived column. The stemming, stop words and language handling of the OpenSearch analyzer then apply to a Lance-backed column:

```json
POST /_lance/attach
{
  "table": "s3://bucket/tables/articles.lance",
  "overrides": { "body": { "type": "text_analyzer", "analyzer": "english" } }
}
```

```json
POST /articles/_search
{ "query": { "match": { "body": "running" } } }
```

matches documents whose `body` contains `run`, `runs` or `running` (the `english` analyzer stems both sides).

What the attach writes:

- A derived Utf8 column named `<column>__lance_tokens` unless the override declares a `derived_column_name`, backfilled with the column's values run through the declared `analyzer`; tokens joined by single spaces, one AddColumns commit.
- `analyzer` is a built-in OpenSearch analyzer name such as `standard`, `english`, `whitespace`, or one a bundled analysis plugin registers globally. Index-scoped custom analyzers (defined in another index's settings) are not resolvable at attach; only globally registered analyzers apply.
- A `whitespace`-tokenized inverted index with positions over the derived column.
- The base column maps as `lance_text` with `tokens_column` naming the derived column and `meta.lance_analyzer` recording the analyzer; the derived column itself is not surfaced in the mapping or `_source`.

How queries run:

- `match`, `term`, `lance_match`, `lance_match_phrase`, `lance_multi_match`, `lance_fts_bool` and `lance_fts_boost` on the base field analyze the query text with the same analyzer, join the tokens with spaces, and run against the derived column, so index-time and query-time tokenisation agree (an `english`-analyzed column matches a query for `running` to a stored `run`).
- `match_phrase` keeps phrase order over the analyzed tokens (the derived column's index stores positions).
- `wildcard`, `regexp` and `prefix` keep running over the raw stored string of the base column.
- `lance_multi_match` combines analyzer-mode fields only when they share one analyzer, and refuses a mix of analyzer-mode and native-tokenizer fields. Fields on Lance's native tokenizer and analyzer-mode fields cannot combine in one `multi_match` / `lance_multi_match`.
- `GET /<index>/_lance/explain` shows which column a pushed full text scan reads (`columns=[body__lance_tokens]` once the field is in the analyzer mode).

How the backfill runs:

- The source column is scanned in fragment order, each batch of 4,096 rows is tokenized as one task on the generic thread pool with at most `lance.attach.backfill_threads` running at once (node setting, dynamic, default half the CPUs the JVM sees, at least 1), and the token batches are handed to Lance's `AddColumns` in source order, which writes them into the table's own data files.
- Nothing is spooled to a local disk and no free space is checked on the node: the backfill needs no disk of its own, and memory stays bounded by the batches in flight (twice the thread count, source text plus tokens).
- The table grows by about the size of the source column (the derived column stores the tokens, joined by spaces), and then by the inverted index over it.
- Expected rate: about 165,000 rows per second per thread for 250 byte English values with the `english` analyzer, so a 20M row column takes about 8 s of tokenizing on 16 threads before Lance writes the column and builds its index. The Lance side of `AddColumns` and the index build are Lance's own cost and are not affected by the thread setting.
- The backfill blocks the attach call by default. A `derive: sync` attach can be stopped with the tasks API (`POST _tasks/<id>/_cancel`); the backfill stops between two batches and the pending `AddColumns` commits nothing.
- `derive: async` on the attach body answers at once, runs the backfill on a background thread, and lets the freshness check flip the mapping to the analyzer mode. An async backfill runs to completion or fails on its own (one WARN line naming the table).
- The async response carries a `backfill` object: `estimated_bytes` (about how much the derived columns add to the table, the average UTF-8 length of the first 4,096 values times the row count times 1.2), `spool_path` (`"none"`: the backfill writes into the table only) and `threads`.
- A failed or interrupted backfill leaves no derived column, and re-attaching retries it from the start. Re-attaching is idempotent: an existing derived column is left alone and its index build is skipped.
- A snapshot pinned by `version` or `tag` cannot be backfilled; attach it after the derived column exists.

How the mapping flips:

- The backfill is two Lance commits, the derived column and then its inverted index, and the mapping flips at the second one. A derived column without its index is left out of the mapping (the field keeps its interim mapping and a note in the derivation says why), so no query runs a full text scan over the derived column while the index is still being built.
- The flip works whether or not the column already carries a Lance inverted index: from the interim `keyword` mapping of a column without one, the check recreates the index through the established type-change rebuild (the Lance data is untouched); from the interim `lance_text` mapping of a column that has one, the check updates the field in place, adding `tokens_column` and `meta.lance_analyzer`.
- A mapping update the cluster manager refuses leaves the index on its interim mapping. The refusal is reported per index under `freshness.mapping_errors` in `GET /_lance/stats` and as `mapping_error` in the answer of `POST /{index}/_lance/sync`, so an operator who sees no `tokens_column` after the backfill should look there.

### `fields`

`fields` declares `keyword` sub-fields on a Utf8 base column (`lance_text` or `keyword`), so a single Lance column serves both full-text and exact-match / aggregation without duplicating source. Sub-field query resolution goes through Lucene doc values on the base column.

The legacy `multi_fields` clause (`{"body": {"raw": {"type": "keyword"}}}`) is still accepted for one release as an alias: it folds into `overrides.[col].fields` at parse time, and a body declaring sub-fields for the same column through both clauses returns 400.

### Validation

Validation answers 400 naming the column and the reason:

| rule | accepted |
|---|---|
| `type` values | `date`, `keyword`, `ip`, `wildcard`, `geo_point`, `text_analyzer` |
| `type: date` column | signed Int32 / Int64, Date, Timestamp |
| `type: keyword` column | Utf8 (with or without inverted index), List&lt;Utf8&gt; |
| `type: ip` column | Utf8, List&lt;Utf8&gt; (holding IP address strings) |
| `type: wildcard` column | Utf8 |
| `type: geo_point` column | Struct&lt;Float64, Float64&gt; named (lat/lon), (latitude/longitude) or (y/x); or FixedSizeList&lt;Float64&gt;[2] |
| `type: text_analyzer` column | Utf8 |
| `fields` column | Utf8 (resolves to `lance_text`, `keyword` or `ip`) |
| `format` | only with `type: date`, validated as a date format pattern |
| `order` | only with `type: geo_point` on a FixedSizeList column; `lat_lon` (default) or `lon_lat` |
| `analyzer` | required with `type: text_analyzer`; must name a globally registered OpenSearch analyzer |
| `derived_column_name` | only with `type: text_analyzer`; must differ from the column and must not collide with a non-Utf8 column |
| keys under a column | `type`, `format`, `order`, `analyzer`, `derived_column_name`, `fields` |
| column | must exist in the table and must not be the primary key |

### Persistence

- `POST /_lance/namespace` accepts the same `overrides` object and applies it to every table it surfaces under the root. A column a table lacks is skipped for that table (logged at debug) while the full list is persisted, so the override applies once a later manifest adds the column.
- Overrides are persisted as canonical JSON in the `index.lance.overrides` index setting. The freshness check's re-derivation reads the setting back and re-applies it on every manifest version advance, so overrides survive schema changes; an override whose column disappears is kept in the setting and skipped until the column returns.
- Indexes created before this setting existed keep resolving their sub-fields from the legacy `index.lance.multi_fields` setting.
- The setting is dynamic because the check itself rewrites it when the table renames an overridden column (the override follows the column, see [Schema drift](features.md#schema-drift)) or resets one to a type the override no longer fits. An operator can also edit it with `PUT /{index}/_settings`, but a manual edit only takes effect at the next mapping re-derivation and reader reopen, so re-attaching is the predictable way to change overrides by hand.

## Index type selection

The attach body and the namespace register body accept an `indexes` clause next to `overrides`, selecting the Lance index type the build creates per column. Without it a scalar column gets a BTree index and a vector column an IVF_PQ index:

```json
POST /_lance/attach
{
  "table": "s3://bucket/tables/demo.lance",
  "indexes": {
    "category": { "scalar": "bitmap" },
    "price":    { "scalar": "zonemap", "params": { "rows_per_zone": 8192 } },
    "flag":     { "scalar": "none" },
    "embedding":{ "vector": "ivf_hnsw_sq", "params": { "num_partitions": 256, "m": 16, "ef_construction": 100 } }
  }
}
```

Accepted types per kind, with the `params` keys each takes (values are numbers; unknown keys return 400 naming the accepted keys). `none` builds no index on the column, even where the automatic build would:

| kind | type | params | prefer it when |
|---|---|---|---|
| scalar | `btree` (default) | `zone_size` | general-purpose equality and range |
| scalar | `bitmap` | | low-cardinality columns (categories, flags) |
| scalar | `zonemap` | `rows_per_zone` | range scans over data sorted or clustered on the column |
| scalar | `bloomfilter` | `number_of_items`, `probability` | equality probes on high-cardinality columns |
| scalar | `ngram` | | substring matching on Utf8 columns |
| scalar | `labellist` | | membership tests on List&lt;Utf8&gt; columns |
| vector | `ivf_pq` (default) | `num_partitions`, `num_sub_vectors`, `num_bits`, `sample_rate` | large tables, memory-bounded search |
| vector | `ivf_flat` | `num_partitions`, `sample_rate` | small tables, exact distances within a partition |
| vector | `ivf_sq` | `num_partitions`, `num_bits`, `sample_rate` | scalar-quantised compromise between size and recall |
| vector | `ivf_rq` | `num_partitions`, `num_bits` | RaBitQ-style quantisation |
| vector | `ivf_hnsw_pq` | `num_partitions`, `m`, `ef_construction`, `num_sub_vectors`, `num_bits`, `sample_rate` | graph search over PQ codes |
| vector | `ivf_hnsw_sq` | `num_partitions`, `m`, `ef_construction`, `num_bits`, `sample_rate` | recall-leaning graph search |

Which columns take which kind:

- `scalar` is accepted on scalar columns (signed integers, floats, booleans, Date / Timestamp, Utf8, List&lt;Utf8&gt;), `vector` on FixedSizeList&lt;Float32&gt; columns; the wrong kind returns 400 naming the column and its Arrow type. One column declares one kind.
- Lance keeps several scalar index types on one column, and the build compares the requested type with the types the column carries: a `zonemap` requested on a column that already has a BTree (a writer's, or the default build's) is built next to it, while the type the column already carries is skipped and `optimize: true` extends it over new fragments.
- Whether the column's data type suits the chosen index (for example `bloomfilter` takes no boolean columns) is Lance's decision at build time and surfaces under `failed` with Lance's message.
- FTS (inverted) indexes are not selected here; their options stay `fts_columns` / `tokenizer` / `with_position` on `build_indexes` ([Building an FTS index](features.md#building-an-fts-index-and-choosing-its-tokenizer)).

Vector defaults and minimums:

- The distance type is L2. `num_partitions` defaults to 1 for every vector type; `ivf_pq` has `num_sub_vectors` 8 and `num_bits` 8.
- The product-quantised types (`ivf_pq`, `ivf_hnsw_pq`) need `2^num_bits` rows to train the codebook (256 with the default 8 bits), and every IVF type needs at least `num_partitions` rows for k-means to form its partitions. A table below the minimum reports the column under `skipped` with the type and the bound.

Where the preference lives:

- The preference persists inside the `index.lance.overrides` setting (under a top-level `indexes` key), so the re-derivation on manifest version advance carries it like the mapping overrides, and the namespace register applies it leniently per table (a table without the column skips it).
- `POST /_lance/build_indexes/{index}` accepts the same `indexes` object in its body as a one-shot override of the persisted preference for that build only; nothing is persisted. `optimize: true` merges whatever index exists regardless of its type.
- `GET /_lance/stats` reports, per index, the Lance index types present per column under `indices.<index>.index_types` (`{"rating": ["ZoneMap"], "embedding": ["IVF_FLAT"]}`, read from `describeIndices` once per stats call), so an operator can verify the preference took effect. When the index has a reader wrapper installed (a security plugin's DLS/FLS wrapper), `index_types` is empty: Lance's index metadata does not pass through the wrapper, and the report must not reveal column names the wrapper hides.
