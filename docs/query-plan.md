# Query plan

This page is the reference for the plan the plugin builds for a search body: how the coordinator
plans it, how a data node refines what it receives, how to read the `_lance/explain` answer, how
the cost the planner compares is computed, what the two traits mean, and the wire format the plan
travels in. [architecture.md](architecture.md#planner-design) explains why the planner exists and
how it is put together; [features.md](features.md#query-plan-preview) lists the endpoint next to
the other features. This page assumes you have a Lance backed index and a search body in hand.

- [Overview](#overview)
- [The explain endpoint](#the-explain-endpoint)
- [Physical operators](#physical-operators)
- [Cost](#cost)
- [Refinements](#refinements)
- [Fragment pruning](#fragment-pruning)
- [Traits](#traits)
- [Wire format](#wire-format)

## Overview

A search body against a Lance backed index is planned once, on the coordinating node, before any
data node sees it. `RequestPlanner` rewrites the query with the same shard free rewrite the shard
path applies, translates the whole body (query, sort, page, aggregations) into one relational
tree over the table scan (`SearchRequestToRel`), and hands that tree to a Calcite Volcano planner.
Pushdown rules fold what the Lance scan can compute into the scan node; converter rules produce
the Lucene alternative for the same tree; the planner keeps the candidate with the lowest cost
that declares the traits the request demands. The winning per node subtree is written down as a
`FragmentPlan` and shipped with every per node request. The data nodes run no planner: each
applies four node local guards (`FragmentPlanRefiner`) that can move a pushed operation back to
Lucene, then executes. `GET /<index>/_lance/explain` runs exactly the coordinator's planning
entry with the same inputs and renders what it produced, without executing anything.

Two vocabularies appear throughout. The Calcite one: a logical tree (what the translator built),
a physical tree (what the planner chose), operators, traits and costs. The plugin one: the fragment
route (the coordinator fans the request out to the data nodes) and the unsupported route (no
plan answers the body, which holds `suggest` or `highlight`; a search refuses it), the pushed scan, the
Lucene operators, the refinements, the fragment pruning and the three stats counters
`plan.refinements`, `plan.executed` and `plan.pruned` under `GET /_lance/stats`.

## The explain endpoint

```
GET /<index>/_lance/explain
{ ...a search body... }
```

The body is any search body the index accepts. The endpoint answers 404 for an unknown index, 400
for an index that is not Lance backed, and 400 for the one body the runtime refuses as well (a
filtered `lance_knn` whose filter has no Lance SQL form). Every other body answers 200 with the
plan, including a body the search endpoint refuses with `plan_failed` (see
[Traits](#traits)): explain describes the refusal instead of repeating it.

Example, for `{"size": 0, "aggs": {"s": {"sum": {"field": "id"}}}}` against a single node cluster
and a six row table (the pushed aggregate returns one group row, so the scan below the fitted
model's range is charged one millisecond, and the coordinator layer adds its two constants):

```json
{
  "index": "demo",
  "route": "fragment",
  "logical": "LanceAggregate(group=[{}], s=[SUM($0)], metrics=[[SUM{name=s}]])\n  LanceTableScan(table=[[lance, demo]])\n",
  "physical": "MergeExec(reduce=[AGGREGATE_INTERNAL], accuracy=[EXACT], tie_stability=[UNSTABLE], cost=[{ms=1, native_bytes=1, heap_bytes=0}], total_cost=[{ms=3, native_bytes=3, heap_bytes=0}])\n  FanOutExec(fanOut=[1], partitioning=[EQUAL_FRAGMENT_GROUPS], accuracy=[EXACT], tie_stability=[UNSTABLE], cost=[{ms=1, native_bytes=1, heap_bytes=0}])\n    LanceTableScan(table=[[lance, demo]], pushed=[[aggregate{groups=0, buckets=[], metrics=[SUM{name=s}]}]], accuracy=[EXACT], tie_stability=[UNSTABLE], cost=[{ms=1, native_bytes=1, heap_bytes=0}])\n",
  "fragment_plan": {
    "kind": "PUSHED_SCAN",
    "aggregate": {
      "group_count": 0,
      "metrics": [{"name": "s", "kind": "SUM"}],
      "substrait_bytes": 290
    }
  },
  "refinements_possible": [],
  "traits": {
    "requested": {"accuracy": "APPROXIMATE", "tie_stability": "NONE"},
    "declared": {"accuracy": "EXACT", "tie_stability": "UNSTABLE"},
    "enforcer": "none"
  }
}
```

The fields, in the order they appear.

`index` is the index the body was planned against.

`route` is `fragment` when the coordinator fans the body out to the data nodes, `unsupported`
when the body carries an element no plan answers (`suggest` or `highlight`, see
[limitations.md](limitations.md#search-body-elements-the-plugin-refuses)). On the unsupported
route `unplanned` carries the message a `_search` with the same body is refused with (400), and
`logical`, `physical`, `fragment_plan`, `refinements_possible` and `traits` are absent, since
nothing was planned; the endpoint answers 200 because it reports rather than executes. The
dispatch filter's mixed target check (a target that is not Lance backed sends the whole request to
the stock search action) is applied outside the plan and is not reflected in `route`.

`logical` is the tree the translator built, one operator per line, indented by depth, as Calcite
prints it. `physical` is the tree the planner chose, printed the same way. On the fragment route
the physical root is the coordinator's `MergeExec` over `FanOutExec` over the per node plan; at
execution the fan out width is lower when the table has fewer fragments than data nodes and
higher when a node's share exceeds the Lucene reader bound. Every physical operator line carries
three extra terms: `accuracy` and `tie_stability` (the [traits](#traits) the operator declares)
and `cost` (what the planner charged that operator, see [Cost](#cost)); the root carries
`total_cost` as well, the sum over the whole tree. The `logical` text carries none of the three:
it is the translator's tree before any cost was computed, and its operators (`LanceAggregate`,
`LanceHitShape`, `LanceTopK`, `LogicalFilter`, the bare `LanceTableScan`) print their own terms
only. In the `physical` text a logical operator the planner kept (a tree it could not lower)
prints the same way, without the three terms. The request's `query` clause plans as a filter over the scan; a query filter under an aggregation the
pushdown computes folds into one pushed aggregate whose `filter=` names the Lance SQL the scan
evaluates; a top level Lance FTS clause (`lance_match`, `lance_match_phrase`, `lance_multi_match`,
`lance_fts_bool`, `lance_fts_boost`, or a stock `match` / `match_phrase` / `multi_match` on a
`lance_text` field, which the coordinator rewrites to the matching `lance_*` clause before planning;
alone, or inside a `bool` whose `must` and `should` hold full text clauses only, fused into one
`lance_fts_bool` when there are several, with scalar `filter` / `must_not` companions) and a top level
`lance_knn` (optionally with its inner `filter`)
plan as their own logical nodes and show on the scan as pushed `fts` / `knn` operations carrying
every parameter and the filter's SQL. Do not parse the text; its shape will keep changing as the
planner grows.

`fragment_plan` is the `FragmentPlan` the coordinator would ship with every per node request, the
same object a data node logs under `lance.plan` at debug level. `kind` is `PUSHED_SCAN` when the
scan computes the page or the aggregate, `LUCENE_AGGREGATE`, `LUCENE_TOPK` or `LUCENE_COUNT`
when Lucene's aggregators, collector or count run over the planned query. `filter_sql` is the
Lance SQL of the scalar predicate, or of the prefilter of a full text or knn clause, absent when
the query has no Lance spelling. `lance_clause` is the query name of the full text or knn clause
the executor builds its Lance query from (`lance_knn`, `lance_match`, ...), absent for a scalar
shape. `top_k` describes a pushed page (`orderings` with `column`, `ascending`, `nulls_first`;
`fetch`; `cursor_sql` for a `search_after` page) and `aggregate` a pushed aggregate
(`group_count`, `metrics` with the aggregation `name` and metric `kind`, `substrait_bytes`).
`excluded_fragment_ids` lists, in ascending order, the fragments the coordinator's zone map
pruning proved empty of rows matching the query predicate (see
[Fragment pruning](#fragment-pruning)); the executors leave them out of every scan of the
request. It is absent when nothing was pruned: no scalar predicate, no zone map on a predicate
column, or every fragment may hold a match.
Compare it with the node's `lance.plan:` line or with `plan.refinements`, `plan.executed` and
`plan.pruned` in `GET /_lance/stats` to see whether a data node executed the shipped plan,
downgraded it, and how many fragments it skipped. The field is absent when the plan failed
(below).

`unplanned` names the request element that kept the envelope, or the whole query, on the Lucene
side: the translator's message for the first element it could not spell (`query type [match]` for
a `match` on a field that is not `lance_text`, `full text clause in [should] next to [filter]
without a [must] clause`, `sort type [_geo_distance]`, `aggregation type [top_hits]`, `column
[body] behind aggregation field [body] is not numeric`), or the structural reason (`size [5] (only 0 with aggregations)`,
`aggregations with a post_filter`, `pipeline aggregation`, `min_score or terminate_after (applied
by the Lucene collectors)`). It is absent when everything translated, including when the cost
model chose the Lucene operator for a tree that did translate. When no plan meets the request's
trait demand it carries the `plan_failed` message the search endpoint answers 400 with.

`refinements_possible` lists the node local downgrades a data node could still apply to the
shipped plan, predicted from the mapping and the plan, in the order of the
[refinement reasons](#refinements): `security_wrapper` before `sort_field_type` before
`aggregate_resolution` before `column_store_warm`. Only the first two are predicted today (the
other two depend on inputs only the data node has), so the array holds zero, one or two entries
and reads the same for every caller. It is empty when none applies and absent on the unsupported
route.

`traits` summarises the trait side of the plan, see [Traits](#traits): `requested` is what the
body demanded of the plan root, `declared` what the root declares, `enforcer` whether the second
planning pass fired.

When no plan of the body declares the traits it demands, the answer keeps `route: fragment` but
nothing ships: `fragment_plan` is absent, `refinements_possible` is empty, `unplanned` carries
the `plan_failed` message, `physical` shows the cheapest plan the demand refused (so the reader
sees which trait it declares), and `traits.enforcer` names the demand and what the cheapest plan
offered. A `_search` with the same body answers 400 with the same message.

## Physical operators

One line per operator, naming what it does and the convention it belongs to. The convention says
where the work runs; converting between conventions is a costed step the planner takes explicitly.

| Operator | Convention | What it does |
| --- | --- | --- |
| `LanceTableScan` | Lance | One Lance scan per fragment group, carrying its pushed operations: `filter{sql=...}`, `fts{...}`, `knn{...}`, `topk{...}`, `aggregate{...}`. The only operator of the convention. |
| `LuceneAggregateExec` | Lucene | The stock OpenSearch aggregators over the per fragment leaf readers, run by the fragment executor's aggregation phase; its terms print the aggregate it wraps and the filter below it. |
| `HeapTopKExec` | Lucene | Lucene's top docs collector cutting the page, run by the executor's hits phase; its terms print the collations, bounds, cursor, hit envelope and the filter, FTS or knn below it. |
| `LuceneHandoffExec` | Lucene | The zero cost conversion of a Lance scan to the Lucene root the planner demands; unwrapped by the planner factory, so it never appears in the printed plan. |
| `FanOutExec` | Lucene (coordinator) | One per node request per fragment group; `fanOut` is the width, `partitioning` how the fragments are cut (`EQUAL_FRAGMENT_GROUPS`: round robin over the sorted data node list, a node's share split further only when its rows exceed what one Lucene reader may hold). Run by the plan executor as the transport fan out. |
| `MergeExec` | Lucene (coordinator) | The reduce of the per node answers: `AGGREGATE_INTERNAL` (the stock aggregation reduce, pipelines included), `HITS_TOP_K` (the sorted page merge) or `COUNT_SUM`. Run by the plan executor as the merge reducer. |

The Lance convention is where the native scan computes; the Lucene convention is where Lucene's
machinery over the fragment leaf readers computes, plus the coordinator's distribution. A plan
folded entirely into the scan reaches the Lucene root through the handoff, so both physical forms
of a tree compete under one root. A body with no plan under either convention (`suggest`,
`highlight`) never reaches the planner: the translator refuses it first.

## Cost

The planner compares candidates by `LanceCost`, which reads Calcite's three cost slots with its
own meaning: predicted latency in milliseconds (the objective), predicted native bytes and
predicted heap bytes (budgets checked as hard constraints against the node's
`lance.native_memory.limit` and the JVM heap; a candidate within both budgets always beats one
outside them, and among candidates on the same side of the budgets milliseconds decide). Nothing
predicts real byte usage yet, so the two byte slots carry small placeholder figures.

The `cost` term on every physical operator line is that operator's own charge
(`computeSelfCost`), printed as `{ms=..., native_bytes=..., heap_bytes=...}` with each number
rounded to two significant digits (`1200`, `1.2`, `0.0012`; `inf` for an infinite cost). The
`total_cost` term on the root is the cumulative cost of the tree, which is the figure the Volcano
run compared the winning candidate by, plus the coordinator layer's two constants (`MergeExec`
and `FanOutExec` each charge one millisecond per unit; the fan out charges one unit per node).

Two regimes decide what the milliseconds mean. For an aggregation over a table of a million rows
or more (`CostCoefficients.FITTED_MODEL_MIN_ROWS`), the pushed scan and the Lucene aggregator
operator are each priced by `plan/cost/CostModel` as a sum of coefficient times quantity terms: a
fixed cost per request, an object store open latency when the table URI is `s3://`, `gs://`,
`az://` or the like, the per row work of every thread (the table's rows divided by the fan out
node count and by the path's parallelism) with one coefficient per kind of group key and metric,
the object store transfer of the columns the scan reads (per node, not per thread), a hash table
penalty above a million groups, and the executor's merge of its parallel scans' group rows. The
coefficients live in `plan/cost/CostCoefficients.java`; they were fitted by non negative least
squares to the warm latencies measured on the 20M, 100M and 1B row benchmark tables across one to
six node clusters, `scripts/fit-cost-coefficients.py` reproduces the fit from
`src/test/resources/cost/measurements.csv`, and `CostModelTests` holds the model to the measured
choices. Below a million rows, and for every hits tree at every size (sorted pages, full text,
vector), the operators keep placeholder costs: the scan charges its estimated rows (or its groups
for a pushed aggregate) as milliseconds, the Lucene operators a constant pinned above the pushed
form so the pushed form wins whenever a rule folds the tree, and a tree with a `cardinality`
metric carries a placeholder penalty on the pushed side so the small table choice matches the
fitted model's choice on the large ones. A `LanceTableScan` with no pushed operation over a table
in the fitted range costs zero, because the Lucene operator above it carries the whole charge.

The quantities come from the tree and from the table statistics the planner collects once per
manifest version from Lance metadata (rows, deleted rows, the bitmap distinct count of a terms
key, the Arrow type widths of the columns read; `GET /_lance/stats` reports the cache under
`plan.statistics`). The run's inputs come from the caller as `plan/cost/CostInputs`: the number of
data nodes the request fans out to, the storage kind of the table URI, this node's CPUs, and four
settings read at their current values, `lance.aggregation.pushdown_parallelism` (the scan's
parallelism), `lance.fragment_path.slices` (the aggregator path's parallelism),
`lance.aggregation.pushdown` and `lance.aggregation.pushdown_max_groups`. The coordinator and
the explain endpoint build their inputs through the same `RequestPlanner.clusterInputs`, so the
two plan a body the same way on the same cluster; a data node planning for itself (in tests)
uses one local node. The same body can therefore explain differently on a single node and on a
six node cluster, and the cost printed by explain is the cluster's figure, not a node's.

The two aggregation routing settings are cost inputs, not gates in front of the planner.
`lance.aggregation.pushdown: false` prices every pushed aggregate as infinite (`cost=[{ms=inf,
...}]` never appears in a winning plan, so explain shows the `LuceneAggregateExec` alternative
with nothing `unplanned`). `lance.aggregation.pushdown_max_groups` is applied when the planner
can estimate the group rows the executor would hold from the statistics (every key domain known:
a bitmap distinct count, a date interval, a range or filter count, cut to the `shard_size`
retention of a single count or metric ordered `terms` level): an estimate above the bound prices
the pushed scan as infinite too. A key without statistics leaves the tree to the executor's own
bound (see `aggregate_resolution` below).

What changes cost between an explain answer and the run that follows: the data node count (a node
joined or left), the storage kind (a table re-registered from a different URI), the four settings
above, and the table's row count and statistics as the manifest advances. What the model does not
see at all: the state of the Lance index cache, whether the aggregator path's columns are warm in
a node's column store (decided on the data node, see `column_store_warm`), and concurrent
requests (the coefficients are single request latencies).

## Refinements

The data node does not plan; it checks the shipped `FragmentPlan` against what only it knows and
downgrades where a pushed operation cannot or should not run there. Downgrades go one way, from
the Lance scan to Lucene, so the plan the coordinator ships is an upper bound on what the scan
computes. `FragmentPlanRefiner.refine` applies four guards, in this order, each on the plan as
the previous guards left it.

1. `security_wrapper`. A reader wrapper on the index service (the security plugin's DLS / FLS)
   sends a pushed aggregate, a pushed page and a pushed full text clause to the aggregators, the
   collector and the Lucene composition of the query, because the Lance scan does not see the
   wrapper's filter. A pushed `lance_knn` and the scalar filter SQL survive.
2. `sort_field_type`. A pushed page whose sort a Lucene sort field cannot type the sort values
   from (an `ip` sort format, or a `search_after` cursor equal to a sort field's missing value
   sentinel) goes to the collector.
3. `aggregate_resolution`. A pushed aggregate whose fields do not resolve against the mapping, or
   whose group estimate from the request shape (the product of the levels' `shard_size`; a
   `range` or `filters` level counts its bucket count plus one) exceeds
   `lance.aggregation.pushdown_max_groups`, goes to the aggregators.
4. `column_store_warm`. Over an object store table, a pushed aggregate goes to the aggregators
   when the node's column store already holds every column they would read for every fragment of
   the request and the coordinator's shipped prediction for the aggregators over resident columns
   is below its prediction for the scan. The coordinator ships both predictions and the column
   names next to the pushed aggregate. Over a local table the resident cost equals the cost the
   planner already compared, so this never fires there; below the fitted range the shipped costs
   are zero. Nothing warms the column store at attach (`lance.attach.warm_indexes` warms the
   Lance index cache, not the column store); once a Lucene side request has loaded the aggregated
   columns of an object store table into the store (an aggregation the planner did not push, a
   run with `lance.aggregation.pushdown: false`), the node answers later pushed eligible
   aggregations over the same table from the store instead of scanning the object store again.

Each guard that fires increments its counter under `plan.refinements` in `GET /_lance/stats`, and
`plan.executed` next to it counts, per node, the requests the Lance scan answered (`pushed_scan`:
an ordered, limited page or a Substrait aggregate) and the ones Lucene's collector and aggregators
answered (`lucene`, which includes a pushed page cut in the scan's own order and every full text
or `lance_knn` page, since the collector cuts those over the fused Lance query). The executor
logs the planned and the executed plan at debug level under `lance.plan`. Two consequences of the order are worth knowing when
reading the counters. A `FragmentPlan` carries a pushed page or a pushed aggregate, never both, so
`sort_field_type` never fires together with `aggregate_resolution` or `column_store_warm`. And
`security_wrapper`, when it fires, removes the pushed aggregate and the pushed page before the
later guards look, so guards 2 to 4 are skipped for that request: a node with a reader wrapper
counts `security_wrapper` alone even for a request whose aggregate would also have failed
resolution. The counters therefore do not add up across reasons; each counts the reason that
actually caused the downgrade. The explain endpoint predicts the first two reasons from what the
coordinating node can see (whether a reader wrapper is installed, whether the page orders by an
`ip` override column) and lists them in the same order under `refinements_possible`.

## Fragment pruning

Before the plan ships, the coordinator checks the query predicate against the zone maps of the
table (`ZoneMapPruner`) and lists the fragments no matching row can come from in
`FragmentPlan.excludedFragmentIds` (`excluded_fragment_ids` in the explain answer). A zone map
index (`"scalar": "zonemap"` in the attach's `indexes` clause, or a zone map built on the table
by another writer) records per zone, a run of `rows_per_zone` rows inside one fragment, the
column's minimum, maximum and null count. A fragment is excluded when every one of its zones is
proven empty: for a comparison (`term`, `terms`, `range`, the bounds of a `date` term) the
literal falls outside the zone's `[min, max]`, or the zone is all null; for `exists`
(`IS NOT NULL`) the zone is all null; for an `IS NULL` the zone holds no null. A `bool`'s `must`
and `filter` clauses exclude a fragment when any clause does, its `should` clauses only when
every clause does, and a `must_not` clause is never read (it keeps rows without a value, and the
pruner does not reason about negation). The predicate is the query's, read from the logical
tree, so the exclusions apply whatever physical form the planner chose: a pushed page, a pushed
aggregate, the prefilter of a full text or `lance_knn` clause, the Lucene collectors, and the
count path all skip the same fragments. A `post_filter` is not read (it applies to the hits
alone, not to the aggregations).

Every judgement is conservative. A fragment the zone map does not cover (appended after the index
was built, or rewritten by a compaction the index has not followed) is kept; so is a zone whose
bounds are unknown (a null minimum or maximum next to non null rows), a floating point zone
whose maximum is `NaN` (Lance ranks `NaN` above every finite value, so the finite maximum is
hidden), a predicate shape the pruner does not read (`wildcard`, `regexp`, `prefix`, a negation,
a column without a zone map), and every fragment when the statistics could not be collected.
Zone bounds compare in the column's own representation: an integer, date or timestamp bound as
Lance reports it (a day count for `date32`, the column's unit for a timestamp), widened to the
enclosing milliseconds before it meets the epoch millis literal a date range translates to; a
string bound by code point, the order Lance computed the bounds in. Excluding a fragment never
changes an answer, only the fragments the executors open.

The zone maps are read lazily, once per manifest version and per column, and only for the columns
the query names: the coordinator reads them while it holds the table open to enumerate the
fragments (`TableStatistics.readZoneMaps`), before the planner runs, and the entry in the
statistics cache keeps them for the life of that version. A table without a zone map index
costs nothing here; a request whose query names no zone mapped column reads nothing.

Each data node's executor removes the excluded fragments from the list the coordinator sent it
before it opens the fragment reader and issues any Lance scan, and counts the fragments it
skipped under `plan.pruned.fragments` in `GET /_lance/stats`. A node whose every fragment is
excluded still answers the request, over a reader with no leaves, so the aggregations block and
the count the coordinator merges have the shape an empty table produces. The Lance side counts
(`hits.total` of a scalar filter or a full text prefilter) read the same reduced list.

Only the zone map's minimum, maximum and null count are consulted. A bloom filter index, which
answers an equality probe on a high cardinality column, and the fragment coverage of a BTree
are not read for pruning.

## Traits

Two request demandable traits sit next to the convention in every operator's trait set
(`plan/traits/`). A physical operator declares the value it guarantees; a request demands a value
at the plan root; the planner only accepts a plan whose root declares a value that satisfies the
demand. The refinements keep the traits by construction (a Lucene form of an operation declares
the same values as its pushed form), so what the coordinator declares holds on the data node.

`Accuracy` says whether an operator's figures are exact or come from a sketch. `EXACT` is every
hit page, every count and every aggregate whose metrics are sums, averages, extremes, value
counts, stats or bucket counts. `APPROXIMATE` is an aggregate carrying a `cardinality`
(HyperLogLog++), `percentiles` or `percentile_ranks` (t-digest) metric, which the pushed scan and
the Lucene aggregators compute as the same sketches, so both physical forms of such a tree
declare the same value. `EXACT` satisfies a demand for either value; `APPROXIMATE` satisfies only
itself. The trait def's default (what a root demands when the request asks for nothing, and what
a logical operator declares) is `APPROXIMATE`, the weakest value.

`TieStability` says whether the order of rows that compare equal under the request's sort is
reproducible between two calls. `STABLE_ROWADDR` is Lance row address order: a bare scan and a
page without a sort over a scalar query (Lucene doc order over the whole table reader is the same
order). `STABLE_KEY` is the order of a stored column with ties resolved
the same way on every call: a page whose last collation is a stored column, on the pushed scan
and on `HeapTopKExec` alike, whether or not a score collation precedes it. `UNSTABLE` is a page
cut in score order alone out of a full text or knn scan, whose equal scores land in whatever
order the scanner's batches arrive, and every aggregate output, whose group rows have no order
contract. Each stable value satisfies a demand for itself and for `UNSTABLE`; the two stable
values do not satisfy each other, because a cursor typed against one order cannot be evaluated in
the other. The default is `UNSTABLE`.

`LuceneHandoffExec`, `FanOutExec` and `MergeExec` inherit their input's values: the handoff moves
rows unchanged, the fan out moves per node rows, and the merge sums exact counts into an exact
count, reduces sketches into a sketch and keeps the per node pages' tie order. So the root of the
coordinator tree declares what the per node plan declares, and `traits.declared` reads the same
as the innermost operator's terms.

Two request elements demand a trait (`RequestPlanner.requirementOf`), and `traits.requested`
shows the result. An explicit `track_total_hits` (an integer bound or `true`) demands
`Accuracy.EXACT`; without it `requested.accuracy` is `APPROXIMATE`, the default, meaning no
demand. A `search_after` cursor over a page the tree carries demands `TieStability.STABLE_KEY`;
without a cursor `requested.tie_stability` is `NONE`. A cursor over a page the translator kept on
Lucene (a sort without a collation spelling, a `collapse`) places no demand either, because the
executor's collector builds the page and the cursor from the request's own sort. A query outside
the translator's vocabulary places no demand: the Lucene collectors answer it as they always did,
and the `declared` values are those of the placeholder bare scan the plan carries.

When each value is achievable: `EXACT` for everything except a tree with a sketch metric, on
either physical form; `STABLE_KEY` for a page whose last sort key is a stored column;
`STABLE_ROWADDR` for a page with no sort over a scalar query (no request spells it as a demand
today, since `_rowaddr` is not a sort field). A `track_total_hits` bound next to a `cardinality`
or `percentiles` aggregation, and a `search_after` cursor over a page sorted by `_score` alone,
demand values no plan of the body declares.

The enforcer is the second planning pass. `LancePlannerFactory.planUnder` runs the Volcano pass
once for the convention alone; when its cheapest plan already declares the demanded values (the
common case) it is returned and `traits.enforcer` reads `none`. Otherwise the same cluster is
asked again with the demanded values on the root trait set, so a costlier plan that declares
them wins over the cheaper one that does not, and `traits.enforcer` names the request element,
the demanded value and what the cheapest plan offered, ending in `a costlier plan declaring the
demand was chosen`. When no plan declares them, Calcite's `CannotPlanException` on that second
root becomes `UnmetPlanRequirementException`: the search endpoint answers 400 with its
`plan_failed` message, and explain answers the plan failed shape described above, whose
`traits.enforcer` ends in `no plan declares the demand (plan_failed)`. Neither trait has a Calcite
converter (nothing raises a sketch to an exact figure or makes a score order reproducible after
the fact), so a demand no operator meets is refused rather than converted.

## Wire format

Two objects of the plan travel between nodes, and both are internal to the plugin. `FragmentPlan`
travels from the coordinator to every data node inside the per node fragment request, and back to
nobody: the data node logs it and executes it. `LanceExplainResponse` travels from the node that
planned the explain body to the node that received the REST call when they differ.

Both streams open with an integer `WIRE_VERSION` (`FragmentPlan.WIRE_VERSION` is `2` today,
`LanceExplainResponse.WIRE_VERSION` is `2`), written first and read first through the
`WireVersion` helper. A reader that finds another number refuses the stream with an `IOException`
naming both numbers (`FragmentPlan wire version [3] does not match this node's [2]: every node must
run the same plugin version`), so a mismatch fails at the first field with a message that says why,
instead of misreading the fields that follow into a generic stream corruption error. The number is
bumped whenever a field is added, removed or retyped. No reader decodes an older number: the marker
detects a mismatch, it does not negotiate one, and the fragment request between two plugin versions
fails.

The same marker opens every other plugin internal message that crosses nodes, each with its own
`WIRE_VERSION` constant and its own name in the message. The fragment request and response around
the plan (`LanceFragmentQueryRequest`, `LanceFragmentQueryResponse`) carry one for the fields they
add around it: the request's marker guards the table, storage options, pinned version and the
Lucene side builders, the plan's marker guards the plan, so a change to either bumps its own
number. The per node stats request and the stats a node returns (`LanceStatsNodeRequest`,
`LanceNodeStats`, the whole payload of `LanceStatsNodeResponse`), the per node `node_local` build
request and response (`LanceBuildIndexesNodeRequest`, `LanceBuildIndexesNodeResponse`), the sync
request and response the shard's node answers (`LanceIndexSyncRequest`, `LanceIndexSyncResponse`),
and the poll, namespace update and attach requests and responses the cluster manager answers
(`LanceNamespacePollRequest`, `LanceNamespacePollResponse`, `LanceNamespaceUpdateRequest`,
`LanceNamespaceUpdateResponse`, `LanceAttachRequest`, `LanceAttachResponse`) each open with theirs.
The marker sits after the fields the OpenSearch base class writes (the parent task id of a request,
the cluster manager timeout of a cluster manager request, the node of a per node response, the
acknowledged bit of an acknowledged response), which OpenSearch versions itself, and before the
first field the plugin owns. A `Writeable` nested in one of these messages without a marker of its
own (`LanceBuildIndexesRequest` inside the node request, `KindResult` inside the node response, the
records inside `LanceNodeStats`) is covered by the enclosing message's number, so a change to its
fields bumps that number. Messages the plugin only executes on the node that received the REST
call (`LanceExplainRequest`, `LanceRefsRequest`, `LanceNamespaceListRequest`,
`LanceBuildIndexesRequest` at the top level, and their responses) carry no marker: a
`HandledTransportAction` invoked through the node client never serialises them.
`LanceNamespaceMetadata` carries none either: it is cluster state, versioned and published by
OpenSearch's own mechanism, and its backwards-compatibility policy is written on the class.

The contract that follows is the plugin's for now: every node in the cluster runs the same plugin
version, and a rolling upgrade is not supported for the fragment path. The
backwards-compatibility policy on `LanceNamespaceMetadata` states the same for the cluster state
the plugin writes. When the plugin has releases to upgrade between, a reader can branch on the
marker to decode the previous format; the marker is where that branch goes.

After the marker, `FragmentPlan` writes its kind, the optional filter SQL, the optional Lance
clause as a named writeable query builder, the optional pushed page (orderings, fetch, cursor
SQL), the optional pushed aggregate (the Substrait bytes, the group count, the metric slots,
the two shipped cost predictions and the column names the aggregators would read) and the
excluded fragment ids as an integer array (empty when nothing was pruned).
`LanceExplainResponse` writes the index, the route, the two optional plan texts, the optional
fragment plan, the optional unplanned message, the predicted refinements and the optional traits
object (the requested accuracy, whether a tie stability was demanded and which, the declared pair,
and the enforcer text); on the unsupported route only the index, the route and the message are
set.
