# Mapping overrides

The attach body and the namespace register body take an optional `overrides` clause next to the table: per column mapping rules that change the OpenSearch type the plugin derives from the Arrow schema. It persists in the `index.plugins.lance.overrides` index setting and is re-applied on every manifest version advance. [features.md](features.md#mapping-type-coverage) lists the types the derivation picks without an override.

- [Mapping overrides](#mapping-overrides)
  - [`type: date`](#type-date)
  - [`type: keyword`](#type-keyword)
  - [`type: ip`](#type-ip)
  - [`type: wildcard`](#type-wildcard)
  - [`type: geo_point`](#type-geo_point)
  - [`fields`](#fields)
  - [Validation](#validation)
  - [Persistence](#persistence)

## Mapping overrides

The attach body accepts an `overrides` clause with per-column mapping rules. Six kinds are supported: a `date` type override, a `keyword` type override, an `ip` type override, a `wildcard` type override, a `geo_point` type override, and keyword sub-fields (`fields`). `type` and `fields` may appear together on one column:

```json
POST /_plugins/_lance/attach
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

- An inverted index the table has on the column stays untouched on the Lance side; this index just does not use it.
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
- Like the `keyword` override, the column's inverted index, if any, is not used.

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

### `fields`

`fields` declares `keyword` sub-fields on a Utf8 base column (`lance_text` or `keyword`), so a single Lance column serves both full-text and exact-match / aggregation without duplicating source. Sub-field query resolution goes through Lucene doc values on the base column.

The legacy `multi_fields` clause (`{"body": {"raw": {"type": "keyword"}}}`) is still accepted for one release as an alias: it folds into `overrides.[col].fields` at parse time, and a body declaring sub-fields for the same column through both clauses returns 400.

### Validation

Validation answers 400 naming the column and the reason:

| rule | accepted |
|---|---|
| `type` values | `date`, `keyword`, `ip`, `wildcard`, `geo_point` |
| `type: date` column | signed Int32 / Int64, Date, Timestamp |
| `type: keyword` column | Utf8 (with or without inverted index), List&lt;Utf8&gt; |
| `type: ip` column | Utf8, List&lt;Utf8&gt; (holding IP address strings) |
| `type: wildcard` column | Utf8 |
| `type: geo_point` column | Struct&lt;Float64, Float64&gt; named (lat/lon), (latitude/longitude) or (y/x); or FixedSizeList&lt;Float64&gt;[2] |
| `fields` column | Utf8 (resolves to `lance_text`, `keyword` or `ip`) |
| `format` | only with `type: date`, validated as a date format pattern |
| `order` | only with `type: geo_point` on a FixedSizeList column; `lat_lon` (default) or `lon_lat` |
| keys under a column | `type`, `format`, `order`, `fields` |
| column | must exist in the table and must not be the primary key |

### Persistence

- `POST /_plugins/_lance/namespace` accepts the same `overrides` object and applies it to every table it surfaces under the root. A column a table lacks is skipped for that table (logged at debug) while the full list is persisted, so the override applies once a later manifest adds the column.
- Overrides are persisted as canonical JSON in the `index.plugins.lance.overrides` index setting. The freshness check's re-derivation reads the setting back and re-applies it on every manifest version advance, so overrides survive schema changes; an override whose column disappears is kept in the setting and skipped until the column returns.
- Indexes created before this setting existed keep resolving their sub-fields from the legacy `index.plugins.lance.multi_fields` setting.
- The setting is dynamic because the check itself rewrites it when the table renames an overridden column (the override follows the column, see [Schema drift](features.md#schema-drift)) or resets one to a type the override no longer fits. An operator can also edit it with `PUT /{index}/_settings`, but a manual edit only takes effect at the next mapping re-derivation and reader reopen, so re-attaching is the predictable way to change overrides by hand.
