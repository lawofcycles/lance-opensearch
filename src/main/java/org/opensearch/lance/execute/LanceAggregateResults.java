/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.execute;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.Comparators;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.engine.FragmentGroupScan;
import org.opensearch.lance.engine.LanceCancellation;
import org.opensearch.lance.execute.AggregateSpecResolver.Child;
import org.opensearch.lance.execute.AggregateSpecResolver.KeyKind;
import org.opensearch.lance.execute.AggregateSpecResolver.Level;
import org.opensearch.lance.execute.AggregateSpecResolver.LevelKind;
import org.opensearch.lance.execute.AggregateSpecResolver.Metric;
import org.opensearch.lance.execute.AggregateSpecResolver.Source;
import org.opensearch.lance.execute.GroupAggregationState.Candidate;
import org.opensearch.lance.execute.GroupAggregationState.Group;
import org.opensearch.lance.execute.GroupAggregationState.GroupState;
import org.opensearch.lance.execute.GroupAggregationState.GroupTable;
import org.opensearch.lance.execute.GroupAggregationState.MetricBatch;
import org.opensearch.lance.execute.GroupAggregationState.MetricState;
import org.opensearch.lance.execute.GroupAggregationState.Partial;
import org.opensearch.lance.execute.GroupAggregationState.TopKGroups;
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.lance.plan.rel.MetricSpec;
import org.opensearch.lance.plan.substrait.LanceSubstraitProducer;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeKey;
import org.opensearch.search.aggregations.bucket.composite.InternalComposite;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.InternalFilters;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.InternalDateRange;
import org.opensearch.search.aggregations.bucket.range.InternalRange;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.RangeAggregator;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;

import static org.opensearch.lance.execute.AggregateSpecResolver.metadata;
import static org.opensearch.lance.execute.AggregateSpecResolver.thresholds;
import static org.opensearch.lance.execute.ArrowRowValues.asLong;
import static org.opensearch.lance.execute.ArrowRowValues.doubleKeyOf;
import static org.opensearch.lance.execute.ArrowRowValues.longOrZero;
import static org.opensearch.lance.execute.ArrowRowValues.readUtf8;

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

    /** Output column of the per group row count. */
    private static final String COUNT_COLUMN = "n";
    /** Output columns of the bucket keys, one per level or composite source: {@code k0}, {@code k1}, ... */
    private static final String KEY_COLUMN_PREFIX = "k";

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
     * Aggregations plus the row total of the fragments this node
     * scanned, and the number of Lance scans that produced them.
     */
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

    /**
     * Package private so tests can build a minimal instance;
     * production instances come only from {@link #resolve}.
     */
    LanceAggregateResults(ResolvedAggregate resolved) {
        this.resolved = resolved;
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
     * The plan of one Lance scan: the main scan carries every
     * grouping and measure of the request; a percentiles metric adds
     * a bin scan of its own afterwards, grouped by the same bucket
     * keys plus its bin, whose rows carry that metric's bin counts
     * only. {@code min}, {@code max} and {@code width} are the bin
     * bounds of a bin scan.
     */
    private record ScanPlan(ByteBuffer substrait, Metric percentiles, double min, double max, double width) {
        boolean isMain() {
            return percentiles == null;
        }
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
     * through {@link CoreAggregationResults}.
     *
     * <p>The fragments are cut into {@code min(fragments, parallelism)}
     * contiguous groups and every group is scanned with its own copy
     * of the plan ({@link FragmentGroupScan}: the groups after the
     * first on {@code executor}, the first on the calling thread); the
     * partial results are merged per key list in Java before the
     * buckets are built. Lance runs
     * the aggregate of one scan in a single DataFusion partition, so
     * this is what gives a node with many fragments more than one
     * core for the hash aggregation. One fragment, or a parallelism
     * of 1, means one scan over {@code fragmentIds} as given.
     *
     * <p>A tdigest {@code percentiles} / {@code percentile_ranks}
     * takes two rounds: the first scan returns the field's minimum
     * and maximum next to the other measures, then one more round of
     * scans (same fragment groups, same filter) groups the rows by
     * {@code floor((value - min) / width)} with
     * {@code width = (max - min) / bins} and counts them, and the
     * executor feeds each bin's centre and count to the TDigest.
     * Two rounds because the bins cannot be laid out before the
     * bounds are known. A single round with a fixed number of
     * quantiles per node (DataFusion's {@code approx_percentile_cont}
     * or {@code NTILE}) was not taken: the coordinator merges one
     * sketch per node, and per node quantiles cannot be merged into
     * an answer over every node. The bounds are node wide over the
     * rows the filter keeps, so every bucket's histogram shares one
     * bin width and a reported percentile is within that width of
     * the exact value; {@code min == max} makes one bin. A field
     * without a value skips the second round and reports an empty
     * digest, as the aggregator does. Each percentiles metric runs
     * its own second round: grouping several metrics' bins in one
     * scan would multiply the rows by the bins of each.
     *
     * <p>Failures inside Lance (a plan it cannot parse, a function
     * its DataFusion build lacks) propagate: falling back to the
     * aggregator path would hide the regression behind a slow answer.
     *
     * <p>{@code cancellation} is checked before every group scan
     * starts and at every batch boundary inside a scan; a cancelled
     * task ends the pushdown with {@code TaskCancelledException} the
     * same way a failing group does.
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
        List<List<Integer>> fragmentGroups = FragmentGroupScan.splitContiguous(fragmentIds, parallelism);
        FragmentGroupScan scans = new FragmentGroupScan(executor, parallelism, cancellation);
        ScanPlan main = new ScanPlan(resolved.substrait(), null, 0d, 0d, 0d);
        Partial merged = mergePartials(scans.runGroups(fragmentGroups, group -> scan(dataset, group, filterSql, main, cancellation)));
        int scanCount = fragmentGroups.size();
        for (Metric metric : resolved.allMetrics()) {
            if (!metric.isPercentiles()) {
                continue;
            }
            ScanPlan bins = binScan(metric, merged);
            if (bins == null) {
                continue;
            }
            merged.merge(mergePartials(scans.runGroups(fragmentGroups, group -> scan(dataset, group, filterSql, bins, cancellation))));
            scanCount += fragmentGroups.size();
        }
        return assemble(merged, scanCount, dateHistogramPrototype);
    }

    /**
     * Merges the per fragment group partials. The top-k partials of
     * the single level terms shape are folded into one
     * {@link GroupTable}, so a key several scans retained has its
     * counts and metrics summed before the final {@code shard_size}
     * selection re-evaluates it.
     */
    private Partial mergePartials(List<Partial> partials) {
        if (resolved.topK() != null && partials.get(0).topK != null) {
            Partial merged = new Partial();
            GroupTable table = new GroupTable(resolved.keyKinds(), resolved.allMetrics());
            merged.groups = table;
            for (Partial partial : partials) {
                merged.total += partial.total;
                merged.keyedCount += partial.topK.keyedCount();
                partial.topK.mergeInto(table);
            }
            return merged;
        }
        if (partials.size() == 1) {
            return partials.get(0);
        }
        Partial merged = new Partial();
        for (Partial partial : partials) {
            merged.merge(partial);
        }
        return merged;
    }

    /**
     * The bin scan of one percentiles metric from the bounds the main
     * scan returned, node wide over every group; null when no row had
     * a value.
     */
    private ScanPlan binScan(Metric metric, Partial merged) {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        if (merged.metricsOnly != null) {
            min = Math.min(min, merged.metricsOnly.metrics[metric.slot()].min);
            max = Math.max(max, merged.metricsOnly.metrics[metric.slot()].max);
        }
        if (merged.groups != null) {
            min = Math.min(min, merged.groups.metrics.minOf(metric.slot(), merged.groups.size()));
            max = Math.max(max, merged.groups.metrics.maxOf(metric.slot(), merged.groups.size()));
        }
        if (min == Double.POSITIVE_INFINITY) {
            return null;
        }
        // Every value falls into bin 0 when they are all equal; any
        // positive width does that, and keeps the centre at the value.
        // The producer computes the same width from the same bounds.
        double width = max > min ? (max - min) / resolved.percentilesBins() : 1d;
        final double minBound = min;
        final double maxBound = max;
        ByteBuffer bins = LanceSubstraitProducer.toLancePercentilesBins(
            resolved.substrait(),
            metric.slot(),
            min,
            max,
            resolved.percentilesBins()
        )
            .orElseThrow(
                () -> new IllegalStateException(
                    "the Substrait producer refused the percentiles bin scan of ["
                        + metric.name()
                        + "] over ["
                        + minBound
                        + ", "
                        + maxBound
                        + "]"
                )
            );
        return new ScanPlan(bins, metric, min, max, width);
    }

    /**
     * One scan of {@code plan} over {@code fragmentIds} (null: every
     * fragment). The rows are read column-wise: the key and measure
     * vectors are resolved once per batch, every key is encoded
     * into a {@code long} (the value itself, the bits of a double,
     * or a dictionary id for a string) and the counts and metric
     * values go into the primitive columns of the partial's group
     * table, so the row loop allocates no objects however many
     * groups Lance returns. The single level terms shape with a
     * top-k order keeps only the bounded selection instead. A main
     * scan row carries the row count and the metrics' partial
     * values; a bin scan row carries only its metric's bin, and
     * adds nothing to the totals.
     */
    private Partial scan(Dataset dataset, List<Integer> fragmentIds, String filterSql, ScanPlan plan, LanceCancellation cancellation)
        throws Exception {
        ScanOptions.Builder options = new ScanOptions.Builder().substraitAggregate(plan.substrait().duplicate());
        if (fragmentIds != null) {
            options.fragmentIds(fragmentIds);
        }
        if (filterSql != null) {
            options.filter(filterSql);
        }
        Partial partial = new Partial();
        try (LanceScanner scanner = dataset.newScan(options.build()); ArrowReader reader = scanner.scanBatches()) {
            if (resolved.keyCount() == 0) {
                scanMetricsOnly(reader, plan, partial, cancellation);
            } else if (resolved.topK() != null) {
                scanTopK(reader, partial, cancellation);
            } else {
                scanGrouped(reader, plan, partial, cancellation);
            }
        }
        return partial;
    }

    /** A plan without groupings: Lance returns one row per scan, read into a {@link GroupState} as before. */
    private void scanMetricsOnly(ArrowReader reader, ScanPlan plan, Partial partial, LanceCancellation cancellation) throws Exception {
        while (reader.loadNextBatch()) {
            cancellation.checkCancelled();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            FieldVector counts = plan.isMain() ? root.getVector(COUNT_COLUMN) : null;
            for (int row = 0; row < root.getRowCount(); row++) {
                MetricState[] states = new MetricState[resolved.allMetrics().size()];
                for (int i = 0; i < states.length; i++) {
                    Metric metric = resolved.allMetrics().get(i);
                    if (plan.isMain()) {
                        states[i] = metric.read(root, row);
                    } else if (metric == plan.percentiles()) {
                        states[i] = metric.readBin(root, row, plan.min(), plan.max(), plan.width(), resolved.percentilesBins());
                    } else {
                        states[i] = new MetricState();
                    }
                }
                GroupState state = new GroupState(counts != null ? longOrZero(counts, row) : 0L, states);
                partial.total += state.count;
                partial.metricsOnly = partial.metricsOnly == null ? state : partial.metricsOnly.merge(state);
            }
        }
    }

    /** The single level terms shape: every row is offered to the bounded top-k selection. */
    private void scanTopK(ArrowReader reader, Partial partial, LanceCancellation cancellation) throws Exception {
        TopKGroups groups = new TopKGroups(resolved.topK(), resolved.allMetrics());
        partial.topK = groups;
        BytesRefBuilder scratch = new BytesRefBuilder();
        while (reader.loadNextBatch()) {
            cancellation.checkCancelled();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            FieldVector counts = root.getVector(COUNT_COLUMN);
            FieldVector keyVector = root.getVector(KEY_COLUMN_PREFIX + 0);
            MetricBatch[] batches = MetricBatch.resolve(resolved.allMetrics(), root);
            MetricBatch orderBatch = resolved.topK().sortSlot() >= 0 ? batches[resolved.topK().sortSlot()] : null;
            for (int row = 0; row < root.getRowCount(); row++) {
                long count = longOrZero(counts, row);
                partial.total += count;
                if (keyVector.isNull(row)) {
                    // Documents without a value open no bucket.
                    continue;
                }
                long numericKey = 0L;
                BytesRef stringKey = null;
                switch (resolved.topK().keyKind()) {
                    case STRING -> stringKey = readUtf8((VarCharVector) keyVector, row, scratch);
                    case DOUBLE -> numericKey = Double.doubleToLongBits(doubleKeyOf(keyVector, row));
                    case LONG -> numericKey = asLong(keyVector, row);
                }
                double sortValue = orderBatch != null ? orderBatch.rowSortValue(row) : 0d;
                int idx = groups.offer(numericKey, stringKey, count, sortValue);
                if (idx >= 0) {
                    for (MetricBatch batch : batches) {
                        batch.add(groups.metrics, idx, row);
                    }
                }
            }
        }
    }

    /** Every other keyed shape: the full key list is encoded and merged into the partial's group table. */
    private void scanGrouped(ArrowReader reader, ScanPlan plan, Partial partial, LanceCancellation cancellation) throws Exception {
        int keyCount = resolved.keyCount();
        GroupTable table = new GroupTable(resolved.keyKinds(), resolved.allMetrics());
        partial.groups = table;
        long[] encoded = new long[keyCount];
        BytesRefBuilder scratch = new BytesRefBuilder();
        Metric percentiles = plan.percentiles();
        double compression = percentiles != null ? percentiles.compression() : 0d;
        while (reader.loadNextBatch()) {
            cancellation.checkCancelled();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            FieldVector counts = plan.isMain() ? root.getVector(COUNT_COLUMN) : null;
            FieldVector[] keyVectors = new FieldVector[keyCount];
            for (int key = 0; key < keyCount; key++) {
                keyVectors[key] = root.getVector(KEY_COLUMN_PREFIX + key);
            }
            MetricBatch[] batches = plan.isMain() ? MetricBatch.resolve(resolved.allMetrics(), root) : null;
            FieldVector bin = percentiles != null ? root.getVector(percentiles.binColumn()) : null;
            FieldVector binCount = percentiles != null ? root.getVector(percentiles.binCountColumn()) : null;
            for (int row = 0; row < root.getRowCount(); row++) {
                if (counts != null) {
                    partial.total += longOrZero(counts, row);
                }
                long nullBits = 0L;
                boolean opensBucket = true;
                for (int key = 0; key < keyCount && opensBucket; key++) {
                    if (keyVectors[key].isNull(row)) {
                        // Documents without a value open no bucket at
                        // that level; without the outermost value
                        // (or any composite source value) they open
                        // no bucket at all.
                        nullBits |= 1L << key;
                        encoded[key] = 0L;
                        opensBucket = key > 0 && resolved.composite() == null;
                    } else {
                        encoded[key] = encodeKey(keyVectors[key], resolved.keyKind(key), row, resolved.dateInterval(key), table, scratch);
                        // A mask of 0 at the outermost level with no
                        // other bucket to hold it is the same: the
                        // row is in no bucket of the tree.
                        opensBucket = key > 0 || resolved.composite() != null || inSomeBucket(resolved.levels().get(0), encoded[key]);
                    }
                }
                if (!opensBucket) {
                    continue;
                }
                int idx = table.findOrAdd(encoded, nullBits);
                if (counts != null) {
                    table.addCount(idx, longOrZero(counts, row));
                }
                if (batches != null) {
                    for (MetricBatch batch : batches) {
                        batch.add(table.metrics, idx, row);
                    }
                } else if (bin != null && !bin.isNull(row)) {
                    long binRows = longOrZero(binCount, row);
                    if (binRows > 0L) {
                        // count(field), not count(*), in the bin scan:
                        // a row without a value has a null bin.
                        table.metrics.addBin(
                            percentiles.slot(),
                            idx,
                            compression,
                            plan.min(),
                            plan.max(),
                            plan.width(),
                            resolved.percentilesBins(),
                            asLong(bin, row),
                            binRows
                        );
                    }
                }
            }
        }
    }

    /** Whether an encoded key at level 0 puts its row into at least one bucket; every key of a non mask level does. */
    private static boolean inSomeBucket(Level level, long encodedKey) {
        return !level.kind().isMask() || encodedKey != 0L || level.otherBucketKey() != null;
    }

    /** Builds the node's aggregations from the merged partials. */
    private Result assemble(Partial merged, int scans, Function<String, InternalAggregation> dateHistogramPrototype) {
        if (resolved.keyCount() == 0) {
            // Lance returns exactly one row for a plan without
            // groupings, even over zero fragments; the fallback only
            // covers a reader that yielded no batch at all, and a
            // cardinality plan, whose distinct grouping returns no
            // row over no rows.
            GroupState state = merged.metricsOnly != null ? merged.metricsOnly : GroupState.empty(resolved.allMetrics().size());
            return new Result(toAggregations(resolved.topMetrics(), state), merged.total, scans);
        }
        GroupTable table = merged.groups != null ? merged.groups : new GroupTable(resolved.keyKinds(), resolved.allMetrics());
        InternalAggregation aggregation;
        if (resolved.topK() != null) {
            aggregation = assembleTopTerms(table, merged.keyedCount, dateHistogramPrototype);
        } else if (resolved.composite() == null && resolved.levels().size() == 1 && isKeyOrderedTerms(resolved.levels().get(0))) {
            aggregation = assembleKeyedTerms(table, dateHistogramPrototype);
        } else {
            List<Group> groups = table.box();
            aggregation = resolved.composite() != null ? buildComposite(groups) : buildLevel(0, groups, dateHistogramPrototype);
        }
        return new Result(InternalAggregations.from(Collections.singletonList(aggregation)), merged.total, scans);
    }

    private static boolean isKeyOrderedTerms(Level level) {
        return level.kind() == LevelKind.TERMS && InternalOrder.isKeyOrder(((TermsAggregationBuilder) level.builder()).order());
    }

    /**
     * The terms result of the merged top-k candidates: the best
     * {@code shard_size} under the request order become the
     * buckets, every other retained or dropped group's count goes
     * to {@code sum_other_doc_count} (the scans counted every keyed
     * row, retained or not), and the doc count error is left at 0
     * for the reduce to derive from the smallest returned bucket,
     * exactly as it does for an aggregator shard result.
     */
    private InternalAggregation assembleTopTerms(GroupTable table, long keyedCount, Function<String, InternalAggregation> prototype) {
        Level level = resolved.levels().get(0);
        TermsAggregationBuilder terms = (TermsAggregationBuilder) level.builder();
        TermsAggregator.BucketCountThresholds thresholds = thresholds(terms);
        int[] selected = table.selectTop(thresholds.getShardSize(), topOrder(table));
        List<Candidate> candidates = boxSelection(table, selected);
        long otherDocCount = keyedCount;
        for (Candidate candidate : candidates) {
            otherDocCount -= candidate.count();
        }
        // Shards hand the reduce key sorted buckets when the request
        // order is not itself a key order, and a top-k order never is.
        BucketOrder reduceOrder = BucketOrder.key(true);
        candidates.sort(candidateComparator(reduceOrder, level.keyKind()));
        List<InternalAggregations> subAggregations = new ArrayList<>(candidates.size());
        for (Candidate candidate : candidates) {
            subAggregations.add(subAggregations(0, candidate, prototype));
        }
        return termsAggregation(level, terms, reduceOrder, thresholds, otherDocCount, candidates, subAggregations);
    }

    /** The selection order of the top-k shape over the merged group table. */
    private GroupTable.GroupOrder topOrder(GroupTable table) {
        if (resolved.topK().sortSlot() < 0) {
            return (a, b) -> {
                int byCount = Long.compare(table.countAt(b), table.countAt(a));
                return byCount != 0 ? byCount : table.compareKeys(a, b);
            };
        }
        int slot = resolved.topK().sortSlot();
        boolean asc = resolved.topK().sortAsc();
        return (a, b) -> {
            int byValue = Comparators.compareDiscardNaN(table.metrics.sortValue(slot, a), table.metrics.sortValue(slot, b), asc);
            return byValue != 0 ? byValue : table.compareKeys(a, b);
        };
    }

    /**
     * A single {@code terms} level ordered by {@code _key}: the
     * groups are sorted and cut in their primitive columns and only
     * the first {@code shard_size} are boxed into buckets, the same
     * selection {@link #buildTerms} makes over boxed candidates.
     */
    private InternalAggregation assembleKeyedTerms(GroupTable table, Function<String, InternalAggregation> prototype) {
        Level level = resolved.levels().get(0);
        TermsAggregationBuilder terms = (TermsAggregationBuilder) level.builder();
        TermsAggregator.BucketCountThresholds thresholds = thresholds(terms);
        boolean ascending = InternalOrder.isKeyAsc(terms.order());
        GroupTable.GroupOrder order = ascending ? table::compareKeys : (a, b) -> table.compareKeys(b, a);
        int[] selected = table.selectTop(thresholds.getShardSize(), order);
        List<Candidate> candidates = boxSelection(table, selected);
        long otherDocCount = table.totalCount();
        for (Candidate candidate : candidates) {
            otherDocCount -= candidate.count();
        }
        List<InternalAggregations> subAggregations = new ArrayList<>(candidates.size());
        for (Candidate candidate : candidates) {
            subAggregations.add(subAggregations(0, candidate, prototype));
        }
        return termsAggregation(level, terms, terms.order(), thresholds, otherDocCount, candidates, subAggregations);
    }

    /** The selected single key groups as boxed candidates, in selection order. */
    private List<Candidate> boxSelection(GroupTable table, int[] selected) {
        List<Candidate> candidates = new ArrayList<>(selected.length);
        for (int idx : selected) {
            Object key = table.boxKey(0, idx);
            GroupState state = new GroupState(table.countAt(idx), table.metrics.boxAll(idx));
            candidates.add(new Candidate(key, state.count, List.of(new Group(Collections.singletonList(key), state))));
        }
        return candidates;
    }

    /**
     * Folds {@code rows} into the bucket aggregation of level
     * {@code depth}: the rows are grouped by that level's key, each
     * key becomes a bucket candidate with the summed count, and the
     * level's kind decides which candidates become buckets. A mask
     * level has a fixed bucket list instead, and a row joins every
     * bucket whose bit its key carries.
     */
    private InternalAggregation buildLevel(int depth, List<Group> rows, Function<String, InternalAggregation> dateHistogramPrototype) {
        Level level = resolved.levels().get(depth);
        if (level.kind().isMask()) {
            return buildMaskLevel(depth, rows, dateHistogramPrototype);
        }
        LinkedHashMap<Object, List<Group>> byKey = new LinkedHashMap<>();
        for (Group row : rows) {
            Object key = row.keys().get(depth);
            if (key == null) {
                continue;
            }
            byKey.computeIfAbsent(key, ignored -> new ArrayList<>()).add(row);
        }
        List<Candidate> candidates = new ArrayList<>(byKey.size());
        for (Map.Entry<Object, List<Group>> entry : byKey.entrySet()) {
            long count = 0L;
            for (Group row : entry.getValue()) {
                count += row.count();
            }
            candidates.add(new Candidate(entry.getKey(), count, entry.getValue()));
        }
        return switch (level.kind()) {
            case TERMS -> buildTerms(depth, candidates, dateHistogramPrototype);
            case HISTOGRAM -> buildHistogram(depth, candidates, dateHistogramPrototype);
            case DATE_HISTOGRAM -> buildDateHistogram(depth, candidates, dateHistogramPrototype);
            default -> throw new IllegalStateException("unexpected level kind " + level.kind());
        };
    }

    /**
     * The buckets of a {@code range} / {@code date_range} /
     * {@code filters} / {@code filter} / {@code missing} level: every
     * bucket the request names, in request order, with the rows
     * whose mask has its bit; then the other bucket of a
     * {@code filters} with the rows no filter matched. Every bucket
     * is reported, with a count of 0 and the sub aggregations built
     * over no rows when nothing fell in, as the aggregators do.
     */
    private InternalAggregation buildMaskLevel(int depth, List<Group> rows, Function<String, InternalAggregation> prototype) {
        Level level = resolved.levels().get(depth);
        int branches = level.branchKeys().size();
        List<List<Group>> perBucket = new ArrayList<>(level.bucketCount());
        for (int i = 0; i < level.bucketCount(); i++) {
            perBucket.add(new ArrayList<>());
        }
        for (Group row : rows) {
            long mask = (Long) row.keys().get(depth);
            if (mask == 0L) {
                if (level.otherBucketKey() != null) {
                    perBucket.get(branches).add(row);
                }
                continue;
            }
            for (int i = 0; i < branches; i++) {
                if ((mask & (1L << i)) != 0L) {
                    perBucket.get(i).add(row);
                }
            }
        }
        List<Candidate> buckets = new ArrayList<>(level.bucketCount());
        for (int i = 0; i < level.bucketCount(); i++) {
            long count = 0L;
            for (Group row : perBucket.get(i)) {
                count += row.count();
            }
            buckets.add(new Candidate(i < branches ? level.branchKeys().get(i) : level.otherBucketKey(), count, perBucket.get(i)));
        }
        List<InternalAggregations> subAggregations = new ArrayList<>(buckets.size());
        for (Candidate bucket : buckets) {
            subAggregations.add(subAggregations(depth, bucket, prototype));
        }
        return maskAggregation(depth, buckets, subAggregations);
    }

    /**
     * The result of a mask level from its buckets, in order, and
     * their sub aggregations. {@code range} buckets carry the
     * resolved bounds; a {@code filter} / {@code missing} is the one
     * bucket itself.
     */
    private InternalAggregation maskAggregation(int depth, List<Candidate> buckets, List<InternalAggregations> subAggregations) {
        Level level = resolved.levels().get(depth);
        Map<String, Object> metadata = metadata(level.builder());
        switch (level.kind()) {
            case RANGE -> {
                RangeAggregationBuilder range = (RangeAggregationBuilder) level.builder();
                List<InternalRange.Bucket> rangeBuckets = new ArrayList<>(buckets.size());
                for (int i = 0; i < buckets.size(); i++) {
                    RangeAggregator.Range bounds = level.ranges()[i];
                    rangeBuckets.add(
                        new InternalRange.Bucket(
                            bounds.getKey(),
                            bounds.getFrom(),
                            bounds.getTo(),
                            buckets.get(i).count(),
                            subAggregations.get(i),
                            range.keyed(),
                            level.format()
                        )
                    );
                }
                return new InternalRange<>(range.getName(), rangeBuckets, level.format(), range.keyed(), metadata);
            }
            case DATE_RANGE -> {
                DateRangeAggregationBuilder range = (DateRangeAggregationBuilder) level.builder();
                List<InternalDateRange.Bucket> rangeBuckets = new ArrayList<>(buckets.size());
                for (int i = 0; i < buckets.size(); i++) {
                    RangeAggregator.Range bounds = level.ranges()[i];
                    rangeBuckets.add(
                        new InternalDateRange.Bucket(
                            bounds.getKey(),
                            bounds.getFrom(),
                            bounds.getTo(),
                            buckets.get(i).count(),
                            subAggregations.get(i),
                            range.keyed(),
                            level.format()
                        )
                    );
                }
                return InternalDateRange.FACTORY.create(range.getName(), rangeBuckets, level.format(), range.keyed(), metadata);
            }
            case FILTERS -> {
                FiltersAggregationBuilder filters = (FiltersAggregationBuilder) level.builder();
                List<InternalFilters.InternalBucket> filterBuckets = new ArrayList<>(buckets.size());
                for (int i = 0; i < buckets.size(); i++) {
                    Candidate bucket = buckets.get(i);
                    filterBuckets.add(
                        new InternalFilters.InternalBucket((String) bucket.key(), bucket.count(), subAggregations.get(i), filters.isKeyed())
                    );
                }
                return new InternalFilters(filters.getName(), filterBuckets, filters.isKeyed(), metadata);
            }
            case FILTER -> {
                return CoreAggregationResults.filter(level.builder().getName(), buckets.get(0).count(), subAggregations.get(0), metadata);
            }
            case MISSING -> {
                return CoreAggregationResults.missing(level.builder().getName(), buckets.get(0).count(), subAggregations.get(0), metadata);
            }
            default -> throw new IllegalStateException("not a mask level: " + level.kind());
        }
    }

    /**
     * The sub aggregations of one bucket at level {@code depth}: the
     * level's metrics folded over the bucket's rows, and the nested
     * level built from the same rows, in request order.
     */
    private InternalAggregations subAggregations(int depth, Candidate candidate, Function<String, InternalAggregation> prototype) {
        Level level = resolved.levels().get(depth);
        if (level.children().isEmpty()) {
            return InternalAggregations.EMPTY;
        }
        // The rows' own states stay untouched: the bucket's values
        // are folded into a fresh state, so the same rows can feed
        // the nested level next.
        GroupState folded = GroupState.empty(resolved.allMetrics().size());
        for (Group row : candidate.rows()) {
            folded.merge(row.state());
        }
        List<InternalAggregation> aggregations = new ArrayList<>(level.children().size());
        for (Child child : level.children()) {
            if (child.metric() != null) {
                aggregations.add(child.metric().toAggregation(folded));
            } else {
                aggregations.add(buildLevel(depth + 1, candidate.rows(), prototype));
            }
        }
        return InternalAggregations.from(aggregations);
    }

    /** What the aggregators of level {@code depth} report for a bucket that saw no document, in request order. */
    private InternalAggregations emptySubAggregations(int depth) {
        Level level = resolved.levels().get(depth);
        if (level.children().isEmpty()) {
            return InternalAggregations.EMPTY;
        }
        List<InternalAggregation> aggregations = new ArrayList<>(level.children().size());
        for (Child child : level.children()) {
            aggregations.add(child.metric() != null ? child.metric().empty() : emptyLevel(depth + 1));
        }
        return InternalAggregations.from(aggregations);
    }

    /**
     * The empty result of level {@code depth} as the aggregator's
     * {@code buildEmptyAggregation} returns it. Only the empty bucket
     * filling of a histogram parent with {@code min_doc_count} 0
     * reads it. {@code terms} keeps the request order as its reduce
     * order here, unlike a built result, which the aggregator sorts
     * by key. A mask level reports every bucket with a count of 0 and
     * the empty sub aggregations.
     */
    private InternalAggregation emptyLevel(int depth) {
        Level level = resolved.levels().get(depth);
        if (level.builder() instanceof TermsAggregationBuilder terms) {
            TermsAggregator.BucketCountThresholds thresholds = thresholds(terms);
            return termsAggregation(level, terms, terms.order(), thresholds, 0L, List.of(), List.of());
        }
        if (level.builder() instanceof HistogramAggregationBuilder histogram) {
            return histogramAggregation(depth, histogram, List.of());
        }
        if (level.builder() instanceof DateHistogramAggregationBuilder dateHistogram) {
            return dateHistogramAggregation(depth, dateHistogram, List.of());
        }
        List<Candidate> buckets = new ArrayList<>(level.bucketCount());
        List<InternalAggregations> subAggregations = new ArrayList<>(level.bucketCount());
        InternalAggregations empty = emptySubAggregations(depth);
        for (int i = 0; i < level.bucketCount(); i++) {
            String key = i < level.branchKeys().size() ? level.branchKeys().get(i) : level.otherBucketKey();
            buckets.add(new Candidate(key, 0L, List.of()));
            subAggregations.add(empty);
        }
        return maskAggregation(depth, buckets, subAggregations);
    }

    private InternalAggregation buildTerms(int depth, List<Candidate> candidates, Function<String, InternalAggregation> prototype) {
        Level level = resolved.levels().get(depth);
        TermsAggregationBuilder terms = (TermsAggregationBuilder) level.builder();
        BucketOrder order = terms.order();
        TermsAggregator.BucketCountThresholds thresholds = thresholds(terms);
        int shardSize = thresholds.getShardSize();

        // Keep the first shard_size candidates in request order. The
        // queue's top is the candidate that leaves first, so lessThan
        // is "sorts later".
        Comparator<Candidate> candidateOrder = candidateComparator(order, level.keyKind());
        PriorityQueue<Candidate> queue = new PriorityQueue<>(Math.max(1, Math.min(shardSize, candidates.size()))) {
            @Override
            protected boolean lessThan(Candidate a, Candidate b) {
                return candidateOrder.compare(a, b) > 0;
            }
        };
        long otherDocCount = 0L;
        for (Candidate candidate : candidates) {
            otherDocCount += candidate.count();
            queue.insertWithOverflow(candidate);
        }
        List<Candidate> selected = new ArrayList<>(queue.size());
        while (queue.size() > 0) {
            selected.add(queue.pop());
        }
        Collections.reverse(selected);
        for (Candidate candidate : selected) {
            otherDocCount -= candidate.count();
        }
        // Shards hand the reduce key sorted buckets unless the
        // request order is itself a key order.
        BucketOrder reduceOrder;
        if (InternalOrder.isKeyOrder(order)) {
            reduceOrder = order;
        } else {
            reduceOrder = BucketOrder.key(true);
            selected.sort(candidateComparator(reduceOrder, level.keyKind()));
        }
        // Sub aggregations only for the kept buckets: the rows of a
        // dropped bucket, nested levels included, go with it.
        List<InternalAggregations> subAggregations = new ArrayList<>(selected.size());
        for (Candidate candidate : selected) {
            subAggregations.add(subAggregations(depth, candidate, prototype));
        }
        return termsAggregation(level, terms, reduceOrder, thresholds, otherDocCount, selected, subAggregations);
    }

    private InternalAggregation buildHistogram(int depth, List<Candidate> candidates, Function<String, InternalAggregation> prototype) {
        Level level = resolved.levels().get(depth);
        HistogramAggregationBuilder histogram = (HistogramAggregationBuilder) level.builder();
        double interval = histogram.interval();
        double offset = histogram.offset();
        candidates.sort(Comparator.comparingLong(candidate -> (Long) candidate.key()));
        List<InternalHistogram.Bucket> buckets = new ArrayList<>(candidates.size());
        for (Candidate candidate : candidates) {
            // The scan returns floor((value - offset) / interval); the
            // aggregator multiplies back the same way.
            double key = (double) (Long) candidate.key() * interval + offset;
            buckets.add(
                new InternalHistogram.Bucket(
                    key,
                    candidate.count(),
                    histogram.keyed(),
                    level.format(),
                    subAggregations(depth, candidate, prototype)
                )
            );
        }
        return histogramAggregation(depth, histogram, buckets);
    }

    private InternalAggregation histogramAggregation(
        int depth,
        HistogramAggregationBuilder histogram,
        List<InternalHistogram.Bucket> buckets
    ) {
        Level level = resolved.levels().get(depth);
        InternalHistogram.EmptyBucketInfo emptyBucketInfo = null;
        if (histogram.minDocCount() == 0L) {
            emptyBucketInfo = new InternalHistogram.EmptyBucketInfo(
                histogram.interval(),
                histogram.offset(),
                histogram.minBound(),
                histogram.maxBound(),
                emptySubAggregations(depth)
            );
        }
        return new InternalHistogram(
            histogram.getName(),
            buckets,
            histogram.order(),
            histogram.minDocCount(),
            emptyBucketInfo,
            level.format(),
            histogram.keyed(),
            metadata(histogram)
        );
    }

    private InternalAggregation buildDateHistogram(
        int depth,
        List<Candidate> candidates,
        Function<String, InternalAggregation> dateHistogramPrototype
    ) {
        Level level = resolved.levels().get(depth);
        DateHistogramAggregationBuilder dateHistogram = (DateHistogramAggregationBuilder) level.builder();
        candidates.sort(Comparator.comparingLong(candidate -> (Long) candidate.key()));
        List<InternalDateHistogram.Bucket> buckets = new ArrayList<>(candidates.size());
        for (Candidate candidate : candidates) {
            buckets.add(
                new InternalDateHistogram.Bucket(
                    (Long) candidate.key(),
                    candidate.count(),
                    dateHistogram.keyed(),
                    level.format(),
                    subAggregations(depth, candidate, dateHistogramPrototype)
                )
            );
        }
        if (depth == 0) {
            // The prototype carries the rounding, offset, order,
            // min_doc_count, empty bucket info and format the
            // aggregator computed for this request; create() copies
            // them.
            InternalDateHistogram prototype = (InternalDateHistogram) dateHistogramPrototype.apply(dateHistogram.getName());
            return prototype.create(buckets);
        }
        return dateHistogramAggregation(depth, dateHistogram, buckets);
    }

    /**
     * A nested {@code date_histogram} result with the fields the
     * aggregator would set; the level's rounding is the fixed
     * interval or the calendar unit in UTC with no offset, the only
     * shapes the pushdown accepts.
     */
    private InternalAggregation dateHistogramAggregation(
        int depth,
        DateHistogramAggregationBuilder dateHistogram,
        List<InternalDateHistogram.Bucket> buckets
    ) {
        Level level = resolved.levels().get(depth);
        return CoreAggregationResults.dateHistogram(
            dateHistogram.getName(),
            buckets,
            dateHistogram.order(),
            dateHistogram.minDocCount(),
            level.rounding(),
            dateHistogram.minDocCount() == 0L ? emptySubAggregations(depth) : null,
            level.format(),
            dateHistogram.keyed(),
            metadata(dateHistogram)
        );
    }

    private InternalAggregation termsAggregation(
        Level level,
        TermsAggregationBuilder terms,
        BucketOrder reduceOrder,
        TermsAggregator.BucketCountThresholds thresholds,
        long otherDocCount,
        List<Candidate> selected,
        List<InternalAggregations> subAggregations
    ) {
        BucketOrder order = terms.order();
        boolean showError = terms.showTermDocCountError();
        String name = terms.getName();
        Map<String, Object> metadata = metadata(terms);
        DocValueFormat format = level.format();
        int shardSize = thresholds.getShardSize();
        switch (level.keyKind()) {
            case STRING -> {
                List<StringTerms.Bucket> buckets = new ArrayList<>(selected.size());
                for (int i = 0; i < selected.size(); i++) {
                    Candidate candidate = selected.get(i);
                    buckets.add(
                        new StringTerms.Bucket((BytesRef) candidate.key(), candidate.count(), subAggregations.get(i), showError, 0L, format)
                    );
                }
                return new StringTerms(
                    name,
                    reduceOrder,
                    order,
                    metadata,
                    format,
                    shardSize,
                    showError,
                    otherDocCount,
                    buckets,
                    0L,
                    thresholds
                );
            }
            case LONG -> {
                List<LongTerms.Bucket> buckets = new ArrayList<>(selected.size());
                for (int i = 0; i < selected.size(); i++) {
                    Candidate candidate = selected.get(i);
                    buckets.add(
                        new LongTerms.Bucket((Long) candidate.key(), candidate.count(), subAggregations.get(i), showError, 0L, format)
                    );
                }
                return new LongTerms(
                    name,
                    reduceOrder,
                    order,
                    metadata,
                    format,
                    shardSize,
                    showError,
                    otherDocCount,
                    buckets,
                    0L,
                    thresholds
                );
            }
            case DOUBLE -> {
                List<DoubleTerms.Bucket> buckets = new ArrayList<>(selected.size());
                for (int i = 0; i < selected.size(); i++) {
                    Candidate candidate = selected.get(i);
                    buckets.add(
                        new DoubleTerms.Bucket((Double) candidate.key(), candidate.count(), subAggregations.get(i), showError, 0L, format)
                    );
                }
                return new DoubleTerms(
                    name,
                    reduceOrder,
                    order,
                    metadata,
                    format,
                    shardSize,
                    showError,
                    otherDocCount,
                    buckets,
                    0L,
                    thresholds
                );
            }
            default -> throw new IllegalStateException("unexpected key kind " + level.keyKind());
        }
    }

    /**
     * The composite page of this node: every key combination after
     * {@code after} in source order, the first {@code size} of them
     * as buckets, the last one as {@code after_key}.
     */
    private InternalAggregation buildComposite(List<Group> groups) {
        CompositeAggregationBuilder builder = resolved.composite().builder();
        List<Source> sources = resolved.composite().sources();
        List<Group> page = new ArrayList<>();
        for (Group group : groups) {
            if (compareToAfter(group) > 0) {
                page.add(group);
            }
        }
        page.sort(compositeOrder());
        int size = Math.min(builder.size(), page.size());

        List<String> sourceNames = new ArrayList<>(sources.size());
        List<DocValueFormat> formats = new ArrayList<>(sources.size());
        int[] reverseMuls = new int[sources.size()];
        MissingOrder[] missingOrders = new MissingOrder[sources.size()];
        for (int i = 0; i < sources.size(); i++) {
            Source source = sources.get(i);
            sourceNames.add(source.builder().name());
            formats.add(source.format());
            reverseMuls[i] = source.reverseMul();
            missingOrders[i] = source.builder().missingOrder();
        }
        List<InternalComposite.InternalBucket> buckets = new ArrayList<>(size);
        CompositeKey afterKey = null;
        for (int i = 0; i < size; i++) {
            Group group = page.get(i);
            Comparable<?>[] values = new Comparable<?>[sources.size()];
            for (int s = 0; s < sources.size(); s++) {
                values[s] = (Comparable<?>) group.keys().get(s);
            }
            CompositeKey key = CoreAggregationResults.compositeKey(values);
            List<InternalAggregation> metrics = new ArrayList<>(resolved.composite().metrics().size());
            for (Metric metric : resolved.composite().metrics()) {
                metrics.add(metric.toAggregation(group.state()));
            }
            buckets.add(
                CoreAggregationResults.compositeBucket(
                    sourceNames,
                    formats,
                    key,
                    reverseMuls,
                    missingOrders,
                    group.count(),
                    InternalAggregations.from(metrics)
                )
            );
            afterKey = key;
        }
        return CoreAggregationResults.composite(
            builder.getName(),
            builder.size(),
            sourceNames,
            formats,
            buckets,
            afterKey,
            reverseMuls,
            missingOrders,
            metadata(builder)
        );
    }

    /** Source by source comparison in each source's direction, the composite's bucket order. */
    private Comparator<Group> compositeOrder() {
        List<Source> sources = resolved.composite().sources();
        Comparator<Group> order = null;
        for (int i = 0; i < sources.size(); i++) {
            int index = i;
            Source source = sources.get(i);
            Comparator<Group> bySource = (a, b) -> compareKeys(source.keyKind(), a.keys().get(index), b.keys().get(index)) * source
                .reverseMul();
            order = order == null ? bySource : order.thenComparing(bySource);
        }
        return order;
    }

    /**
     * Where {@code group} sorts relative to the request's {@code after}
     * key in the composite order; positive when it belongs to this
     * page. Always positive without an {@code after}.
     */
    private int compareToAfter(Group group) {
        List<Source> sources = resolved.composite().sources();
        if (sources.get(0).after() == null) {
            return 1;
        }
        for (int i = 0; i < sources.size(); i++) {
            Source source = sources.get(i);
            int cmp = compareKeys(source.keyKind(), group.keys().get(i), source.after()) * source.reverseMul();
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    private static InternalAggregations toAggregations(List<Metric> metrics, GroupState state) {
        if (metrics.isEmpty()) {
            return InternalAggregations.EMPTY;
        }
        List<InternalAggregation> values = new ArrayList<>(metrics.size());
        for (Metric metric : metrics) {
            values.add(metric.toAggregation(state));
        }
        return InternalAggregations.from(values);
    }

    /**
     * Ordering of candidates for the terms selection: {@code _count}
     * descending with the key ascending as tie breaker, or the key in
     * the requested direction. Keys compare the way the terms buckets
     * compare them: {@link BytesRef} order for strings, numeric order
     * otherwise.
     */
    private static Comparator<Candidate> candidateComparator(BucketOrder order, KeyKind keyKind) {
        Comparator<Candidate> byKey = (a, b) -> compareKeys(keyKind, a.key(), b.key());
        if (InternalOrder.isKeyOrder(order)) {
            return InternalOrder.isKeyAsc(order) ? byKey : byKey.reversed();
        }
        return Comparator.comparingLong(Candidate::count).reversed().thenComparing(byKey);
    }

    private static int compareKeys(KeyKind keyKind, Object a, Object b) {
        return switch (keyKind) {
            case STRING -> ((BytesRef) a).compareTo((BytesRef) b);
            case LONG -> Long.compare((Long) a, (Long) b);
            case DOUBLE -> Double.compare((Double) a, (Double) b);
        };
    }

    /**
     * The group key encoded as one {@code long}: the (interval
     * multiplied) value of an integer or date key, the
     * {@link Double#doubleToLongBits} of a floating point key (whose
     * bit equality is {@link Double#equals} equality), or the id of a
     * string key in the table's dictionary. The encoding allocates
     * nothing: string bytes go through the reused {@code scratch}.
     */
    private static long encodeKey(FieldVector vector, KeyKind kind, int row, long dateInterval, GroupTable table, BytesRefBuilder scratch) {
        return switch (kind) {
            case STRING -> table.intern(readUtf8((VarCharVector) vector, row, scratch));
            case DOUBLE -> Double.doubleToLongBits(doubleKeyOf(vector, row));
            case LONG -> {
                long value = asLong(vector, row);
                yield dateInterval > 0L ? value * dateInterval : value;
            }
        };
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
