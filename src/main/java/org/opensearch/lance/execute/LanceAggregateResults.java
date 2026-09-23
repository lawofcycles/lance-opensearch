/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.execute;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.Dataset;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.engine.LanceCancellation;
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.lance.plan.rel.MetricSpec;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;

/**
 * Runs a {@code size: 0} aggregation request as a group by inside the
 * Lance scan and turns the returned group rows back into the same
 * {@link InternalAggregation} types the Lucene aggregators would have
 * produced on this node, so the coordinator's
 * {@link InternalAggregations#topLevelReduce} merges them unchanged.
 *
 * <p>The decision that a request pushes down belongs to the planner:
 * the coordinator's translator builds a {@link LanceAggregate}, the
 * pushdown rule asks the Substrait producer for the scan bytes, and
 * the plan the fragment request carries brings the bytes and the
 * aggregate's shape (group key count, metric names and kinds) to the
 * transport action, which hands them here. {@link #resolve} then pairs
 * the request's aggregation builders with that shape and
 * prepares the response-side state the plan does not model (Lucene doc
 * value formats, bucket orders, keyed flags, metadata, the terms top-k
 * selection and the group estimate bound); {@link #execute} runs the
 * scans and assembles the buckets. A null from {@link #resolve} sends
 * the request to the Lucene aggregators.
 *
 * <p>This class is the public surface and the wiring; the work is
 * split by responsibility over four package private collaborators.
 * {@link AggregateSpecResolver} walks the request tree against the
 * mapping and the pushed aggregate and produces a
 * {@link ResolvedAggregate} (the spec records: levels, composite
 * sources, metrics by slot, the top-k selection).
 * {@link AggregateScanRunner} owns the Lance scans: the fragment group
 * fan out, the percentiles bin rounds and the row loops that fill the
 * {@link GroupAggregationState} (the columnar group table, the metric
 * columns, the bounded top-k selection and their merge).
 * {@link AggregationResultAssembler} turns the merged state into the
 * {@link InternalAggregation} objects per kind. The Arrow scalar
 * readers the three share are in {@link ArrowRowValues}.
 *
 * <p>Semantics reproduced from the shard aggregators:
 * <ul>
 *   <li>Rows whose bucket key is null form no bucket at that level
 *       (the aggregators skip documents without a value) but still
 *       count toward the enclosing bucket and toward
 *       {@code hits.total}, which is the sum of {@code count(*)} over
 *       every group. A composite bucket needs every source value.</li>
 *   <li>{@code terms} keeps the top {@code shard_size} groups by the
 *       request order ({@code _count} descending with key ascending as
 *       the tie breaker, or {@code _key}), with the same default
 *       {@code shard_size} of {@code size * 1.5 + 10}. The returned
 *       buckets are sorted by key when the order is not a key order,
 *       {@code sum_other_doc_count} is the count of every other group,
 *       and the doc count error is left at 0 for the reduce to derive
 *       from the last bucket when the node returned {@code shard_size}
 *       buckets, exactly as {@code InternalTerms.reduce} does for a
 *       shard.</li>
 *   <li>{@code histogram} keys arrive as
 *       {@code floor((value - offset) / interval)} ordinals and are
 *       multiplied back here; calendar interval keys are
 *       {@code date_trunc(unit, ts)} in UTC, all sorted ascending;
 *       empty bucket filling for {@code min_doc_count} 0 stays with the
 *       coordinator's reduce, which reads the {@code EmptyBucketInfo}
 *       attached here.</li>
 *   <li>{@code composite} sorts the key combinations by every source
 *       in its order, drops the ones at or before {@code after}, and
 *       returns the first {@code size} with the last one as
 *       {@code after_key}, as {@code CompositeAggregator.buildAggregations}
 *       does for a shard. The coordinator's reduce merges the per node
 *       pages and applies {@code size} again.</li>
 *   <li>Date columns are converted to epoch milliseconds inside the
 *       scan, booleans to 0 / 1, so {@code sum} / {@code min} /
 *       {@code max} on them return the same numbers the doc values
 *       path returns.</li>
 *   <li>The fragments of a node are scanned in up to
 *       {@code lance.aggregation.pushdown_parallelism} groups and the
 *       per group rows are merged by their full key list before any
 *       bucket is built: counts, sums and value counts add, min and
 *       max take the extreme, {@code avg} travels as a sum and a
 *       count. The {@code terms} selection therefore sees the same
 *       groups one scan would have returned, and {@code shard_size},
 *       {@code sum_other_doc_count} and the error bound keep their
 *       single scan meaning.</li>
 * </ul>
 */
public final class LanceAggregateResults {

    /**
     * Bins of a pushed down percentiles histogram, from
     * {@code lance.aggregation.percentiles_bins}; the plugin stores the
     * node setting here at start and every dynamic update after.
     */
    private static volatile int defaultPercentilesBins = LancePlugin.AGGREGATION_PERCENTILES_BINS_SETTING.getDefault(Settings.EMPTY);

    public static void setPercentilesBins(int bins) {
        defaultPercentilesBins = bins;
    }

    /**
     * How many times {@code shard_size} groups each scan of a single
     * level {@code terms} keeps, from
     * {@code lance.aggregation.pushdown_topk_slack}; the plugin stores
     * the node setting here at start and every dynamic update after.
     */
    private static volatile int defaultTopkSlack = LancePlugin.AGGREGATION_PUSHDOWN_TOPK_SLACK_SETTING.getDefault(Settings.EMPTY);

    public static void setTopkSlack(int slack) {
        defaultTopkSlack = slack;
    }

    /**
     * What one pushed aggregation returned: the node's aggregations,
     * the row total over every group and the number of Lance scans it
     * took (fragment groups times rounds).
     */
    public record Result(InternalAggregations aggregations, long totalRows, int scans) {
    }

    /**
     * The shape of the pushed aggregate the plan carries: how many group
     * keys the scan groups by and, per aggregate call in call order, the
     * request's aggregation name and the metric kind. This is what
     * {@link AggregateSpecResolver#resolve} checks the request's builders
     * against.
     */
    public record PushedShape(int groupCount, List<MetricSlot> metrics) {

        public PushedShape {
            metrics = List.copyOf(metrics);
        }

        /** From the pushed operation the planner produced. */
        public static PushedShape of(LanceAggregate aggregate) {
            List<MetricSlot> slots = new ArrayList<>();
            for (MetricSpec spec : aggregate.metricSpecs()) {
                slots.add(new MetricSlot(spec.aggregationName(), spec.kind()));
            }
            return new PushedShape(aggregate.getGroupCount(), slots);
        }
    }

    /** One aggregate call of the pushed aggregate: the request's aggregation name and the metric kind. */
    public record MetricSlot(String name, MetricSpec.Kind kind) {
    }

    private final ResolvedAggregate resolved;
    private final AggregateScanRunner runner;
    private final AggregationResultAssembler assembler;

    /**
     * Package private so tests can build a minimal instance;
     * production instances come only from {@link #resolve}.
     */
    LanceAggregateResults(ResolvedAggregate resolved) {
        this.resolved = resolved;
        this.runner = new AggregateScanRunner(resolved);
        this.assembler = new AggregationResultAssembler(resolved);
    }

    /**
     * The encoded main scan, as a read only view. Package private
     * for the test sources' byte equivalence assertions between
     * two plans of the same request.
     */
    ByteBuffer substraitPlan() {
        return resolved.substraitPlan();
    }

    /**
     * The parsed {@code after} value of every composite source, in
     * source order; null when the plan has no composite, all null
     * entries when the request carried no {@code after}. Package
     * private for the test sources' assertions on the composite
     * paging state, which the wire form does not carry (the
     * executor applies {@code after} to the sorted group rows).
     */
    List<Comparable<?>> compositeAfterValues() {
        return resolved.compositeAfterValues();
    }

    /**
     * Runs the plan. {@code fragmentIds} null means every fragment;
     * {@code filterSql} null means no filter. {@code dateHistogramPrototype}
     * supplies, for a top level aggregation name, the empty
     * {@link InternalDateHistogram} the aggregator would build: its
     * constructor is package private, so a top level
     * {@code date_histogram} attaches its buckets through the
     * prototype's public {@code create(List)}, which also carries the
     * aggregator's own empty sub aggregations. A nested
     * {@code date_histogram} has no aggregator handle and is built
     * through {@link CoreAggregationResults}. The scan fan out over the
     * node's fragments, the second round a tdigest percentiles takes
     * and the cancellation checks are described on
     * {@link AggregateScanRunner#run}.
     */
    public Result execute(
        Dataset dataset,
        List<Integer> fragmentIds,
        String filterSql,
        int parallelism,
        Executor executor,
        LanceCancellation cancellation,
        Function<String, InternalAggregation> dateHistogramPrototype
    ) throws Exception {
        AggregateScanRunner.Scanned scanned = runner.run(dataset, fragmentIds, filterSql, parallelism, executor, cancellation);
        return assembler.assemble(scanned.merged(), scanned.scans(), dateHistogramPrototype);
    }

    /**
     * Resolves the request's aggregation builders against the pushed
     * aggregate through {@link AggregateSpecResolver#resolve} with the
     * node's percentiles bins and top-k slack. Returns null when the
     * executor cannot own the plan (an unmapped field, a group estimate
     * over the bound, a terms order the top-k selection cannot honour),
     * in which case the request stays on the Lucene aggregators.
     *
     * @param shape the pushed aggregate's group key count and metric
     *     slots, as the plan the coordinator shipped carries them
     * @param substrait the encoded main scan, a direct buffer
     * @param aggregations the request's aggregation builders
     * @param schema the dataset's Arrow schema
     * @param multiFields the index's keyword sub-field spec
     * @param qsc the mapping of the index the request targets
     * @param maxGroups the bound on the estimated number of groups
     */
    public static LanceAggregateResults resolve(
        PushedShape shape,
        ByteBuffer substrait,
        AggregatorFactories.Builder aggregations,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        int maxGroups
    ) {
        return resolve(shape, substrait, aggregations, schema, multiFields, qsc, maxGroups, defaultPercentilesBins, defaultTopkSlack);
    }

    /** As above with explicit percentiles bins and top-k slack, for tests. */
    static LanceAggregateResults resolve(
        PushedShape shape,
        ByteBuffer substrait,
        AggregatorFactories.Builder aggregations,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        int maxGroups,
        int bins,
        int slack
    ) {
        ResolvedAggregate resolved = AggregateSpecResolver.resolve(
            shape,
            substrait,
            aggregations,
            schema,
            multiFields,
            qsc,
            maxGroups,
            bins,
            slack
        );
        return resolved == null ? null : new LanceAggregateResults(resolved);
    }
}
