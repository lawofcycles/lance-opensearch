# Aggregation pushdown

A `size: 0` request whose query is `match_all` or a scalar filter and whose aggregation tree has one of the shapes below runs inside the Lance scan. The plugin builds a Substrait `AggregateRel` through the Calcite planner (`LanceSubstraitProducer`) and hands it to `ScanOptions.substraitAggregate`; Lance evaluates the group by and the metrics through the DataFusion aggregate kernel it ships (`datafusion_physical_expr::aggregate`).

That kernel is SIMD-optimized and reads Lance's columnar data files directly, so no JVM aggregator or per-document collection sits between the fragment and the aggregate.

Every other tree runs through OpenSearch's stock aggregators over the fragment leaves ([features.md](features.md#aggregations)); the planner chooses between the two forms by cost ([query-plan.md](query-plan.md#cost)).

- [Settings](#settings)
- [Filter encodings](#filter-encodings)
- [Shapes the scan computes](#shapes-the-scan-computes)
- [How the executor builds the buckets](#how-the-executor-builds-the-buckets)
- [Shapes that stay on the aggregators](#shapes-that-stay-on-the-aggregators)
- [Observing the plan](#observing-the-plan)

## Settings

| setting | scope | default | dynamic | effect |
|---|---|---|---|---|
| `lance.aggregation.pushdown` | node | `true` | yes | `false` prices every pushed aggregate as infinite, so the aggregators answer |
| `lance.aggregation.pushdown_parallelism` | node | half the CPUs the JVM sees, at least 1 | yes, 1 to 32 | fragment groups each executor scans at once; `1` is a single scan per executor |
| `lance.aggregation.pushdown_max_groups` | node | `1000000` | no | bound on the estimated group rows, applied by the planner and by the executor |
| `lance.aggregation.percentiles_bins` | node | `4096` | yes, 16 to 1,000,000 | bins of a pushed down tdigest percentiles histogram |
| `lance.aggregation.pushdown_topk_slack` | node | `4` | yes, 1 to 64 | how many times `shard_size` groups each scan of a single top-k ordered `terms` level retains |

`lance.aggregation.pushdown: false` is a cost input, for before / after comparisons: the planner never chooses the pushed aggregate and explain shows `LuceneAggregateExec` with nothing `unplanned`.

## Filter encodings

A scalar filter (`term`, `terms`, `range`, `exists`, `wildcard`, `regexp`, `prefix`, `bool` of those) that stands alone in the plan, or that Lucene's collector or aggregators run over, reaches the Lance scan in one of two encodings, registered as alternatives of one plan and chosen by the cost model.

- The Lance SQL string the planner's `RexToLanceSql` prints (`ScanOptions.filter`).
- The Substrait `ExtendedExpression` bytes the planner's `LanceSubstraitFilterProducer` builds through Calcite and isthmus (`ScanOptions.substraitFilter`), the same pipeline the aggregation pushdown uses.
- Substrait is the default. SQL is chosen only when the Substrait bytes multiplied by the number of data nodes exceed 50 KB, which for a `terms` list of short keyword values is about 4,000 values on one data node and about 1,000 on four. A 200 value list, under 3 KB of Substrait, ships as Substrait on any cluster of up to 18 data nodes. The coefficients are in [query-plan.md](query-plan.md).
- Lance decodes either into the same DataFusion expression, plans it through the same scalar indexes and evaluates it the same way, so the results and `hits.total` do not depend on the choice.
- The Substrait encoding addresses columns by position rather than by name and accepts expressions the SQL printer has no spelling for (arithmetic in a comparison).
- The SQL encoding is the only one for a predicate on a struct child (`parent.child`), for the prefilter of a full text or `lance_knn` clause, for a pushed sorted page and for the filter under a pushed aggregate.
- `GET /<index>/_lance/explain` shows the choice as `filter_substrait_bytes` next to `filter_sql` in `fragment_plan`.
- When the scan carries a filter, Lance's scalar index resolver consults the BTree, Bitmap and Zone Map indexes on the filtered columns automatically and prunes fragments and pages before the aggregate runs.

## Shapes the scan computes

A `size: 0` request whose query is `match_all` or a scalar filter the coordinator translates to Lance SQL (`term`, `terms`, `range`, `exists`, `bool` of those) and whose aggregation tree is one of the following runs as a Substrait `AggregateRel` inside the Lance scan. Each executor asks Lance for one row per group over its fragments, builds the same per node `InternalAggregation` the aggregators would have built, and the coordinator reduces them with the stock reduce. `hits.total` comes from the same scan's `count(*)`.

- Metric aggregations only, any number: `sum`, `avg`, `min`, `max`, `value_count`, `stats`, `extended_stats` (`count`, `sum`, `sum(x * x)`, `min`, `max` as measures) and tdigest `percentiles` / `percentile_ranks`. The tdigest sketch is approximate; see [percentiles](#percentiles). A tree carrying a `cardinality` stays on the aggregators; see [cardinality](#cardinality).
- A chain of up to three bucket aggregations, with metric children at every level and at most one nested bucket per level (`terms > terms > avg`, `range > terms`, `filters > date_histogram`). Each level is one of:
  - `terms` with `order` `_count` descending or `_key`, or, on a single terms level with no nested bucket, one of its own single value metric children; default `min_doc_count`, no `include` / `exclude`.
  - `histogram` with `offset` 0, no `extended_bounds` / `hard_bounds`.
  - `date_histogram` with `fixed_interval`, or `calendar_interval` `second` / `minute` / `hour` / `day` / `week` / `month` / `quarter` / `year` on a timestamp column without a zone or in UTC; `offset` 0, no bounds, no `time_zone`.
  - `range` / `date_range` with 1 to 62 ranges; `from` / `to` bounds resolved by the field's format as the aggregator resolves them, `keyed`, named and unnamed ranges.
  - `missing`.
  - `filter` and `filters` with 1 to 62 filters, `other_bucket` / `other_bucket_key`, whose queries are `match_all`, `term`, `terms`, `range`, `exists` or a `bool` of those over mapped scalar fields.
- One `composite` whose sources are `terms` (either order) or `date_histogram` (a fixed length interval, `offset` 0, no `time_zone`) with `missing_bucket` false, with metric children. The executor sorts the key combinations in source order, drops the ones at or before `after`, and returns the first `size` with the last as `after_key`, as a shard does; the coordinator's reduce merges the per node pages.
- Fields must be mapped and backed by a scalar column: `keyword` on `utf8` (a keyword sub-field resolves to its base column), the integer types, `float` / `double`, `boolean`, `date`.

How a pushed tree behaves:

- The scan groups by every key at once; the executor folds the rows into the tree, so an outer bucket's `doc_count` and metrics cover every row of its key, including the rows the nested level has no value for.
- A `range` / `filters` level groups by the bit set of the ranges / filters a row falls in, so a row in two overlapping ranges counts in both, as with the aggregators, and every range / filter is reported even when empty.
- In the plan, the query filter and the aggregation tree fold into one pushed aggregate on the scan; the filter's Lance SQL is attached to that pushed aggregate (`filter=...` in the explain output) and never enters the Substrait bytes, so the pushed operation the explain endpoint prints is the scan the executor runs.
- The coordinator plans the whole request once and ships the pushed aggregate with its filter SQL to the executors, so the plan the explain endpoint prints is the plan that runs.
- A plan Lance rejects fails the request instead of falling back, so a missing function or a schema mismatch surfaces as an error rather than as a slow answer.

### `terms`

- `terms` keeps the top `shard_size` groups per executor (default `size * 1.5 + 10`), with `sum_other_doc_count` and `doc_count_error_upper_bound` following the shard rules, so a three node answer carries the same error bound a three shard index would.
- A nested `terms` applies the same rules inside each parent bucket, and the rows of a parent bucket the truncation drops are dropped with it.
- `histogram` and `date_histogram` return every bucket; `min_doc_count: 0` filling stays with the coordinator's reduce.
- Rows with a null bucket key open no bucket at that level but count toward the enclosing bucket and `hits.total`.
- A single `terms` level ordered by `_count` descending or by one of its own single value metric children (`sum`, `avg`, `min`, `max`, `value_count`; named as `m` or `m.value`) does not hold every group: each scan keeps the best `shard_size * lance.aggregation.pushdown_topk_slack` groups in a primitive heap and adds every other group's count to `sum_other_doc_count`.
  - The merge sums the counts a key earned in different scans before the final `shard_size` cut, and the doc count error the coordinator derives from the smallest returned bucket covers a key a scan dropped the way it covers a term a shard did not return.
  - A slack large enough to retain every group makes the result exact.
  - Plans with a `percentiles` metric, `_key` orders, nested levels and `composite` keep every group.
  - A metric order on a nested `terms` level stays on the aggregators.

### `percentiles`

tdigest `percentiles` / `percentile_ranks` take two rounds of scans: the first returns the field's minimum and maximum (with the other measures), the second groups the rows by equal width bin over `[min, max]` with `lance.aggregation.percentiles_bins` bins and counts them. The executor feeds each bin's rows to the TDigest of the request's `compression` as one value at each bin edge and the rest at the bin's centre.

- Every value the digest sees is within half a bin width (`(max - min) / bins / 2`) of a real value, so the histogram itself is accurate to the bin width. The TDigest then adds its own interpolation error, which is larger for a digest built from a few thousand weighted points than for one built from every document.
  - Measured: on a 20M row table with a long tailed `price`, the pushdown's p95 was 0.16 % of the range from the exact value, the aggregators' 0.01 %; both p50 within 0.03 %.
- Every bucket of the request shares one node wide bin width; each percentiles metric adds one round of scans.

### `cardinality`

`cardinality` runs through the aggregators. The planner enumerates the pushed form of any aggregation tree carrying one like any other tree and prices it above the Lucene aggregator, so the cost comparison sends the tree to the aggregator path.

- The pushed form asks Lance for a `group by` on the field and feeds each distinct value into the HyperLogLog++ sketch one by one on a single thread, which measured several times to tens of times slower than Lucene's cardinality aggregator on every measured table while returning the same count.
- The fitted cost model reproduces that on tables of a million rows or more, and below that range the pushed placeholder carries a penalty that keeps the same choice. No rule refuses such trees, so a faster pushed implementation changes the plan by changing the coefficients.
- The count is what the aggregators report for the request's `precision_threshold`, exactly as with the pushdown setting off.

## How the executor builds the buckets

- The executor reads Lance's group rows column-wise into primitive arrays (keys as one `long` each, string keys through a byte dictionary, counts and plain measures in parallel arrays), so a scan over millions of groups allocates no objects per row.
- Each executor scans its fragments in up to `lance.aggregation.pushdown_parallelism` contiguous groups at once (Lance runs the aggregate of one scan on a single thread) and merges the group rows by key before building its buckets, so `shard_size`, `sum_other_doc_count` and the error bound keep the meaning of a single scan per node. The extra scans run on the node's `index_searcher` thread pool; when that pool has no free thread the request's own thread scans the remaining groups.
- Nested `terms` levels multiply the key combinations the scan may return. `lance.aggregation.pushdown_max_groups` bounds them in two places:
  - The coordinator's planner estimates the group rows the executor would hold from the table statistics (the distinct counts of the bitmap indexes, the value ranges of the BTree indexes over integer columns, the date intervals, the range and filter counts, capped at the row count, and cut to the `shard_size` retention of a single count or metric ordered `terms` level) and prices the pushed scan as infinite when the estimate exceeds the bound, so the plan is the Lucene operator and explain shows it.
    - A key whose column has neither a distinct count nor an integer range (no bitmap index and no BTree over an integer column, or a `histogram`) has no statistics estimate, and a tree with such a key is not judged against the bound by the planner.
  - The executor estimates the groups again from the request shape (the product of the levels' `shard_size`; a `range` / `filters` level counts as its bucket count plus one) and refuses the shipped pushed aggregate when its own estimate exceeds the same bound; the refusal is counted as `plan.refinements.aggregate_resolution` in `GET /_lance/stats` and the request runs on the aggregators. This second bound protects the executor's group state when the statistics said fewer groups than the request shape implies, or said nothing.

## Shapes that stay on the aggregators

- Requests with a full-text or `lance_knn` query, a `post_filter`, hits (`size > 0`), or a reader wrapper (security plugin DLS / FLS).
- `terms` on a `list<utf8>` column, arithmetic metrics on `keyword`, `histogram` on `date` / `boolean` fields, `range` on `keyword` / `boolean`, `date_range` on a non date field, scripts and `value_type`.
- A tree carrying a `cardinality` (by cost, see above) or a pipeline aggregation ([features.md](features.md#aggregations)).
- A group estimate above `lance.aggregation.pushdown_max_groups`.

## Observing the plan

- `GET /<index>/_lance/explain` prints the pushed aggregate with its `filter=...`, or `LuceneAggregateExec` with the reason under `unplanned` when the tree stays on the aggregators ([query-plan.md](query-plan.md)).
- `GET /_lance/stats` counts the executor's refusal under `plan.refinements.aggregate_resolution` and the pushed and Lucene answers under `plan.executed`.
- The admission gate judges a pushed aggregate under the `aggregate_scan` kind ([admission.md](admission.md#aggregate_scan)).
