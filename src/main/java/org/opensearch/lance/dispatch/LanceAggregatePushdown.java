/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.LongSupplier;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.common.Rounding;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.lance.engine.FragmentGroupScan;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Cast;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Expression;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.FieldReference;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.ScalarType;
import org.opensearch.lance.query.substrait.SubstraitExpressions;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.BucketUtils;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeKey;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.InternalComposite;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.sort.SortOrder;

/**
 * Runs a {@code size: 0} aggregation request as a group by inside the
 * Lance scan. {@link #plan} decides whether the request's aggregation
 * tree can be expressed as one Substrait {@code AggregateRel} against
 * the table's columns and, when it can, encodes the plan;
 * {@link Plan#execute} hands it to Lance with the request's fragment
 * subset and SQL filter, and turns the returned group rows into the
 * same {@link InternalAggregation} types the Lucene aggregators would
 * have produced on this node, so the coordinator's
 * {@link InternalAggregations#topLevelReduce} merges them unchanged.
 *
 * <p>Shapes: any number of top level metrics ({@code sum}, {@code avg},
 * {@code min}, {@code max}, {@code value_count}); a chain of up to
 * three bucket aggregations ({@code terms}, {@code histogram},
 * {@code date_histogram} with a fixed or a calendar interval), each
 * level carrying metric children and at most one nested bucket; or one
 * {@code composite} over {@code terms} and fixed interval
 * {@code date_histogram} sources with metric children. The structural
 * rules live in {@link LanceAggregationSupport#isPushdownCandidate};
 * this class adds the field checks: every field is mapped, backed by a
 * scalar Lance column of a matching Arrow type ({@code keyword} on
 * {@code Utf8}, integer types on signed {@code Int}, {@code float} /
 * {@code double} on {@code FloatingPoint}, {@code boolean} on
 * {@code Bool}, {@code date} on {@code Date} / {@code Timestamp}), and
 * a keyword sub-field resolves to its base column. {@code List<Utf8>}
 * columns are refused because a group by on the list would count rows,
 * not elements.
 *
 * <p>The scan groups by every bucket key at once (one grouping
 * expression per level or composite source) and computes
 * {@code count(*)} and every metric of every level over the finest
 * groups. The executor then folds the rows back into the request's
 * tree: the rows of one outer key form that outer bucket, its
 * {@code doc_count} and metrics are the sums (minimum, maximum) of the
 * rows' values, and the rows are grouped again by the next key for the
 * nested bucket. {@code terms} truncation to {@code shard_size} happens
 * per level and per parent bucket, and the children of a dropped
 * bucket are dropped with it.
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
 *   <li>{@code histogram} keys are
 *       {@code floor((value - offset) / interval) * interval + offset}
 *       on doubles, fixed interval {@code date_histogram} keys are
 *       {@code floorDiv(millis, interval) * interval} on longs, calendar
 *       interval keys are {@code date_trunc(unit, ts)} in UTC, all
 *       sorted ascending; empty bucket filling for {@code min_doc_count}
 *       0 stays with the coordinator's reduce, which reads the
 *       {@code EmptyBucketInfo} attached here.</li>
 *   <li>{@code composite} sorts the key combinations by every source
 *       in its order, drops the ones at or before {@code after}, and
 *       returns the first {@code size} with the last one as
 *       {@code after_key}, as {@code CompositeAggregator.buildAggregations}
 *       does for a shard. The coordinator's reduce merges the per node
 *       pages and applies {@code size} again.</li>
 *   <li>Date columns are converted to epoch milliseconds inside the
 *       scan with the leaf reader's conversion, booleans to 0 / 1, so
 *       {@code sum} / {@code min} / {@code max} on them return the
 *       same numbers the doc values path returns.</li>
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
final class LanceAggregatePushdown {

    private LanceAggregatePushdown() {}

    /** Output column of the per group row count. */
    private static final String COUNT_COLUMN = "n";
    /** Output columns of the bucket keys, one per level or composite source: {@code k0}, {@code k1}, ... */
    private static final String KEY_COLUMN_PREFIX = "k";
    /** {@code after} values are literals; the aggregators reject {@code now} in them the same way. */
    private static final LongSupplier NO_NOW_IN_AFTER = () -> {
        throw new IllegalArgumentException("now() is not supported in [after] key");
    };

    /**
     * Aggregations plus the row total of the fragments this node
     * scanned, and the number of Lance scans that produced them.
     */
    record Result(InternalAggregations aggregations, long totalRows, int scans) {
    }

    private enum MetricKind {
        SUM,
        AVG,
        MIN,
        MAX,
        VALUE_COUNT
    }

    private enum KeyKind {
        STRING,
        LONG,
        DOUBLE
    }

    /** A mapped field resolved to its Lance column. */
    private record Column(String name, int index, ArrowType type, MappedFieldType fieldType) {
        boolean isDate() {
            return type instanceof ArrowType.Date || type instanceof ArrowType.Timestamp;
        }

        /**
         * A {@code Timestamp} column whose values DataFusion reads on the
         * UTC calendar: no zone, or a zone that names UTC. Arrow stores
         * a zoned timestamp as UTC epoch ticks whatever the zone, so
         * epoch arithmetic is unaffected by it, but {@code date_trunc}
         * truncates on the zone's calendar.
         */
        boolean isUtcTimestamp() {
            if (!(type instanceof ArrowType.Timestamp timestamp)) {
                return false;
            }
            String zone = timestamp.getTimezone();
            return zone == null || zone.equals("UTC") || zone.equals("Etc/UTC") || zone.equals("+00:00");
        }

        boolean isBoolean() {
            return type instanceof ArrowType.Bool;
        }

        boolean isFloating() {
            return type instanceof ArrowType.FloatingPoint;
        }

        boolean isUtf8() {
            return type instanceof ArrowType.Utf8;
        }

        /** The column as a number: dates as epoch millis, booleans as 0 / 1. */
        Expression numericExpression() {
            Expression reference = new FieldReference(index);
            if (isDate()) {
                return SubstraitExpressions.epochMillis(reference, type);
            }
            if (isBoolean()) {
                return new Cast(reference, ScalarType.I64);
            }
            return reference;
        }
    }

    /**
     * One metric aggregation anywhere in the tree. Its measures occupy
     * the result columns {@code prefix} ({@code sum} / {@code min} /
     * {@code max} / {@code value_count}) or {@code prefix + "_s"} and
     * {@code prefix + "_c"} ({@code avg}, requested from Lance as a sum
     * and a count rather than as DataFusion's {@code avg}, so the partial
     * values of several scans and of the rows of an outer bucket add up
     * and {@code InternalAvg} carries both for the coordinator's merge).
     * {@code slot} is the metric's position in the plan's metric list
     * and in every {@link GroupState}.
     */
    private record Metric(String name, MetricKind kind, Column column, DocValueFormat format, Map<String, Object> metadata, int slot) {

        String prefix() {
            return "m" + slot;
        }

        void addMeasures(SubstraitAggregatePlan.Builder builder) {
            Expression value = column.numericExpression();
            ScalarType type = column.isFloating() ? ScalarType.FP64 : ScalarType.I64;
            switch (kind) {
                case SUM -> builder.measure("sum", List.of(value), type, prefix());
                case MIN -> builder.measure("min", List.of(value), type, prefix());
                case MAX -> builder.measure("max", List.of(value), type, prefix());
                case VALUE_COUNT -> builder.measure("count", List.of(new FieldReference(column.index())), ScalarType.I64, prefix());
                case AVG -> {
                    builder.measure("sum", List.of(value), type, prefix() + "_s");
                    builder.measure("count", List.of(new FieldReference(column.index())), ScalarType.I64, prefix() + "_c");
                }
            }
        }

        /** The metric's partial values on one group row of one scan. */
        MetricState read(VectorSchemaRoot root, int row) {
            MetricState state = new MetricState();
            switch (kind) {
                case SUM -> state.sum = doubleOrZero(root.getVector(prefix()), row);
                case MIN -> state.min = doubleOr(root.getVector(prefix()), row, Double.POSITIVE_INFINITY);
                case MAX -> state.max = doubleOr(root.getVector(prefix()), row, Double.NEGATIVE_INFINITY);
                case VALUE_COUNT -> state.count = longOrZero(root.getVector(prefix()), row);
                case AVG -> {
                    state.sum = doubleOrZero(root.getVector(prefix() + "_s"), row);
                    state.count = longOrZero(root.getVector(prefix() + "_c"), row);
                }
            }
            return state;
        }

        /** The aggregation the aggregator would report for the merged values. */
        InternalAggregation toAggregation(MetricState state) {
            return switch (kind) {
                case SUM -> new InternalSum(name, state.sum, format, metadata);
                case MIN -> new InternalMin(name, state.min, format, metadata);
                case MAX -> new InternalMax(name, state.max, format, metadata);
                case VALUE_COUNT -> new InternalValueCount(name, state.count, metadata);
                case AVG -> new InternalAvg(name, state.sum, state.count, format, metadata);
            };
        }

        /** This metric's aggregation over the merged values of {@code state}. */
        InternalAggregation toAggregation(GroupState state) {
            return toAggregation(state.metrics[slot]);
        }

        /** The value the aggregator reports for a bucket that saw no document. */
        InternalAggregation empty() {
            return toAggregation(new MetricState());
        }
    }

    /**
     * Running values of one metric over the rows of one group. A fresh
     * state holds the neutral element of every measure (0 for sums and
     * counts, the infinities for min and max), which is also what the
     * aggregator reports for a bucket without documents, so merging a
     * state into a fresh one copies it and merging two partial states
     * adds the sums and counts and keeps the smaller min and larger max.
     */
    private static final class MetricState {
        double sum;
        long count;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;

        void merge(MetricState other) {
            sum += other.sum;
            count += other.count;
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
        }
    }

    /**
     * Running values of one group: its row count and one state per
     * metric of the plan, by slot. The same merge folds the partials of
     * several scans into one group and the rows of one outer key into
     * the outer bucket.
     */
    private static final class GroupState {
        long count;
        final MetricState[] metrics;

        GroupState(long count, MetricState[] metrics) {
            this.count = count;
            this.metrics = metrics;
        }

        /** The state of a group no row contributed to: count 0 and every metric at its neutral element. */
        static GroupState empty(int metricCount) {
            MetricState[] metrics = new MetricState[metricCount];
            for (int i = 0; i < metricCount; i++) {
                metrics[i] = new MetricState();
            }
            return new GroupState(0L, metrics);
        }

        GroupState merge(GroupState other) {
            count += other.count;
            for (int i = 0; i < metrics.length; i++) {
                metrics[i].merge(other.metrics[i]);
            }
            return this;
        }
    }

    /**
     * What one scan returned, and the merge of several. {@code total} is
     * the row count over every group, null keyed rows included; a plan
     * without groupings keeps its single row in {@code metricsOnly}
     * ({@code null} until a row arrived), a plan with groupings keeps
     * one state per key list in {@code groups}. Keys are the objects the
     * bucket types expect ({@link BytesRef}, {@link Long}, {@link Double},
     * or null where the row has no value at an inner level), whose
     * {@code equals} identifies the same group across scans.
     */
    private static final class Partial {
        long total;
        GroupState metricsOnly;
        final Map<List<Object>, GroupState> groups = new HashMap<>();

        void merge(Partial other) {
            total += other.total;
            if (other.metricsOnly != null) {
                metricsOnly = metricsOnly == null ? other.metricsOnly : metricsOnly.merge(other.metricsOnly);
            }
            for (Map.Entry<List<Object>, GroupState> entry : other.groups.entrySet()) {
                groups.merge(entry.getKey(), entry.getValue(), GroupState::merge);
            }
        }
    }

    /**
     * One sub aggregation slot of a bucket level, in request order so
     * the bucket's aggregations come out in the order the aggregators
     * would emit them: a metric, or ({@code metric} null) the nested
     * bucket level.
     */
    private record Child(Metric metric) {
    }

    /**
     * One bucket level of the tree, outermost first. For a
     * {@code date_histogram}, {@code dateInterval} is the
     * {@code fixed_interval} in milliseconds the scan's key ordinal is
     * multiplied back by (0 for a calendar interval, whose key is
     * already the bucket start in millis) and {@code rounding} is the
     * rounding the aggregator would attach to its result; both are 0 /
     * null for the other kinds.
     */
    private record Level(ValuesSourceAggregationBuilder<?> builder, Column column, KeyKind keyKind, DocValueFormat format, List<
        Child> children, List<Metric> metrics, long dateInterval, Rounding rounding) {
    }

    /**
     * One {@code composite} source. {@code after} is the parsed
     * {@code after} value for this source, null when the request has no
     * {@code after}; {@code reverseMul} is 1 for ascending, -1 for
     * descending, as {@code CompositeValuesSourceConfig} names it.
     */
    private record Source(CompositeValuesSourceBuilder<?> builder, Column column, KeyKind keyKind, DocValueFormat format, int reverseMul,
        Comparable<?> after, long dateInterval) {
    }

    private record Composite(CompositeAggregationBuilder builder, List<Source> sources, List<Metric> metrics) {
    }

    /**
     * One merged group: the key of every level or source (a null where
     * the rows have no value at that level) and its row count and
     * metric states.
     */
    private record Group(List<Object> keys, GroupState state) {
        long count() {
            return state.count;
        }
    }

    /** The rows that share one key at one level, before the terms selection decides whether the bucket is built. */
    private record Candidate(Object key, long count, List<Group> rows) {
    }

    /**
     * An encoded plan ready to run. Created by {@link #plan}; a
     * {@code null} plan means the request takes the aggregator path.
     */
    static final class Plan {
        private final ByteBuffer substrait;
        private final List<Level> levels;
        private final Composite composite;
        private final List<Metric> topMetrics;
        private final List<Metric> allMetrics;

        private Plan(ByteBuffer substrait, List<Level> levels, Composite composite, List<Metric> topMetrics, List<Metric> allMetrics) {
            this.substrait = substrait;
            this.levels = levels;
            this.composite = composite;
            this.topMetrics = topMetrics;
            this.allMetrics = allMetrics;
        }

        private int keyCount() {
            return composite != null ? composite.sources().size() : levels.size();
        }

        private long dateInterval(int key) {
            return composite != null ? composite.sources().get(key).dateInterval() : levels.get(key).dateInterval();
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
         * <p>Failures inside Lance (a plan it cannot parse, a function
         * its DataFusion build lacks) propagate: falling back to the
         * aggregator path would hide the regression behind a slow answer.
         */
        Result execute(
            Dataset dataset,
            List<Integer> fragmentIds,
            String filterSql,
            int parallelism,
            Executor executor,
            Function<String, InternalAggregation> dateHistogramPrototype
        ) throws Exception {
            List<List<Integer>> fragmentGroups = FragmentGroupScan.splitContiguous(fragmentIds, parallelism);
            List<Partial> partials = new FragmentGroupScan(executor, parallelism).runGroups(
                fragmentGroups,
                group -> scan(dataset, group, filterSql)
            );
            Partial merged;
            if (partials.size() == 1) {
                merged = partials.get(0);
            } else {
                merged = new Partial();
                for (Partial partial : partials) {
                    merged.merge(partial);
                }
            }
            return assemble(merged, fragmentGroups.size(), dateHistogramPrototype);
        }

        /**
         * One scan of the plan over {@code fragmentIds} (null: every
         * fragment), read into a {@link Partial} keyed by the full key
         * list of every row that opens a bucket.
         */
        private Partial scan(Dataset dataset, List<Integer> fragmentIds, String filterSql) throws Exception {
            ScanOptions.Builder options = new ScanOptions.Builder().substraitAggregate(substrait.duplicate());
            if (fragmentIds != null) {
                options.fragmentIds(fragmentIds);
            }
            if (filterSql != null) {
                options.filter(filterSql);
            }
            int keyCount = keyCount();
            Partial partial = new Partial();
            try (LanceScanner scanner = dataset.newScan(options.build()); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    FieldVector counts = root.getVector(COUNT_COLUMN);
                    FieldVector[] keyVectors = new FieldVector[keyCount];
                    for (int key = 0; key < keyCount; key++) {
                        keyVectors[key] = root.getVector(KEY_COLUMN_PREFIX + key);
                    }
                    for (int row = 0; row < root.getRowCount(); row++) {
                        MetricState[] states = new MetricState[allMetrics.size()];
                        for (int i = 0; i < states.length; i++) {
                            states[i] = allMetrics.get(i).read(root, row);
                        }
                        GroupState state = new GroupState(longOrZero(counts, row), states);
                        partial.total += state.count;
                        if (keyCount == 0) {
                            partial.metricsOnly = partial.metricsOnly == null ? state : partial.metricsOnly.merge(state);
                            continue;
                        }
                        Object[] keys = new Object[keyCount];
                        boolean opensBucket = true;
                        for (int key = 0; key < keyCount && opensBucket; key++) {
                            if (keyVectors[key].isNull(row)) {
                                // Documents without a value open no bucket at
                                // that level; without the outermost value
                                // (or any composite source value) they open
                                // no bucket at all.
                                opensBucket = key > 0 && composite == null;
                            } else {
                                keys[key] = key(keyVectors[key], row, dateInterval(key));
                            }
                        }
                        if (!opensBucket) {
                            continue;
                        }
                        partial.groups.merge(Arrays.asList(keys), state, GroupState::merge);
                    }
                }
            }
            return partial;
        }

        /** Builds the node's aggregations from the merged partials. */
        private Result assemble(Partial merged, int scans, Function<String, InternalAggregation> dateHistogramPrototype) {
            if (keyCount() == 0) {
                // Lance returns exactly one row for a plan without
                // groupings, even over zero fragments; the fallback only
                // covers a reader that yielded no batch at all.
                GroupState state = merged.metricsOnly != null ? merged.metricsOnly : GroupState.empty(allMetrics.size());
                return new Result(toAggregations(topMetrics, state), merged.total, scans);
            }
            List<Group> groups = new ArrayList<>(merged.groups.size());
            for (Map.Entry<List<Object>, GroupState> entry : merged.groups.entrySet()) {
                groups.add(new Group(entry.getKey(), entry.getValue()));
            }
            InternalAggregation aggregation = composite != null ? buildComposite(groups) : buildLevel(0, groups, dateHistogramPrototype);
            return new Result(InternalAggregations.from(Collections.singletonList(aggregation)), merged.total, scans);
        }

        /**
         * Folds {@code rows} into the bucket aggregation of level
         * {@code depth}: the rows are grouped by that level's key, each
         * key becomes a bucket candidate with the summed count, and the
         * level's kind decides which candidates become buckets.
         */
        private InternalAggregation buildLevel(int depth, List<Group> rows, Function<String, InternalAggregation> dateHistogramPrototype) {
            Level level = levels.get(depth);
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
            if (level.builder() instanceof TermsAggregationBuilder) {
                return buildTerms(depth, candidates, dateHistogramPrototype);
            }
            if (level.builder() instanceof HistogramAggregationBuilder) {
                return buildHistogram(depth, candidates, dateHistogramPrototype);
            }
            return buildDateHistogram(depth, candidates, dateHistogramPrototype);
        }

        /**
         * The sub aggregations of one bucket at level {@code depth}: the
         * level's metrics folded over the bucket's rows, and the nested
         * level built from the same rows, in request order.
         */
        private InternalAggregations subAggregations(int depth, Candidate candidate, Function<String, InternalAggregation> prototype) {
            Level level = levels.get(depth);
            if (level.children().isEmpty()) {
                return InternalAggregations.EMPTY;
            }
            // The rows' own states stay untouched: the bucket's values
            // are folded into a fresh state, so the same rows can feed
            // the nested level next.
            GroupState folded = GroupState.empty(allMetrics.size());
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
            Level level = levels.get(depth);
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
         * by key.
         */
        private InternalAggregation emptyLevel(int depth) {
            Level level = levels.get(depth);
            if (level.builder() instanceof TermsAggregationBuilder terms) {
                TermsAggregator.BucketCountThresholds thresholds = thresholds(terms);
                return termsAggregation(level, terms, terms.order(), thresholds, 0L, List.of(), List.of());
            }
            if (level.builder() instanceof HistogramAggregationBuilder histogram) {
                return histogramAggregation(depth, histogram, List.of());
            }
            return dateHistogramAggregation(depth, (DateHistogramAggregationBuilder) level.builder(), List.of());
        }

        private InternalAggregation buildTerms(int depth, List<Candidate> candidates, Function<String, InternalAggregation> prototype) {
            Level level = levels.get(depth);
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
            Level level = levels.get(depth);
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
            Level level = levels.get(depth);
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
            Level level = levels.get(depth);
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
            Level level = levels.get(depth);
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
                            new StringTerms.Bucket(
                                (BytesRef) candidate.key(),
                                candidate.count(),
                                subAggregations.get(i),
                                showError,
                                0L,
                                format
                            )
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
                            new DoubleTerms.Bucket(
                                (Double) candidate.key(),
                                candidate.count(),
                                subAggregations.get(i),
                                showError,
                                0L,
                                format
                            )
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
            CompositeAggregationBuilder builder = composite.builder();
            List<Source> sources = composite.sources();
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
                List<InternalAggregation> metrics = new ArrayList<>(composite.metrics().size());
                for (Metric metric : composite.metrics()) {
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
            List<Source> sources = composite.sources();
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
            List<Source> sources = composite.sources();
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
    }

    /**
     * Resolves the request's aggregation tree against the table schema
     * and the index mapping, with the node's
     * {@code lance.aggregation.pushdown_max_groups} as the group
     * estimate bound. Returns {@code null} when any part of the tree is
     * outside what the scan can compute, in which case the caller runs
     * the Lucene aggregators.
     *
     * @param aggregations the request's aggregation builders, already
     *                     accepted by {@link LanceAggregationSupport#isPushdownCandidate}
     * @param schema       the dataset's Arrow schema (every top level
     *                     column, in order): the Substrait field
     *                     references index into it
     * @param multiFields  the index's keyword sub-field spec, base column
     *                     to sub-field name to type
     * @param qsc          the mapping of the index the request targets
     */
    static Plan plan(
        AggregatorFactories.Builder aggregations,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc
    ) {
        int maxGroups = LancePlugin.AGGREGATION_PUSHDOWN_MAX_GROUPS_SETTING.get(qsc.getIndexSettings().getNodeSettings());
        return plan(aggregations, schema, multiFields, qsc, maxGroups);
    }

    /**
     * As {@link #plan(AggregatorFactories.Builder, Schema, Map, QueryShardContext)}
     * with an explicit bound on the estimated number of groups: the
     * product of the {@code shard_size} of every {@code terms} level.
     * A tree whose estimate exceeds {@code maxGroups} takes the
     * aggregator path, because the scan returns one row per key
     * combination and this node would hold them all.
     */
    static Plan plan(
        AggregatorFactories.Builder aggregations,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        int maxGroups
    ) {
        if (!LanceAggregationSupport.isPushdownCandidate(aggregations)) {
            return null;
        }
        List<AggregationBuilder> top = new ArrayList<>(aggregations.getAggregatorFactories());
        SubstraitAggregatePlan.Builder builder = new SubstraitAggregatePlan.Builder();
        List<Metric> allMetrics = new ArrayList<>();
        if (LanceAggregationSupport.isPushdownMetric(top.get(0))) {
            List<Metric> metrics = resolveMetrics(top, schema, multiFields, qsc, allMetrics);
            if (metrics == null) {
                return null;
            }
            builder.measure("count", List.of(), ScalarType.I64, COUNT_COLUMN);
            for (Metric metric : metrics) {
                metric.addMeasures(builder);
            }
            return new Plan(builder.build(), List.of(), null, metrics, allMetrics);
        }
        if (top.get(0) instanceof CompositeAggregationBuilder compositeBuilder) {
            Composite composite = resolveComposite(compositeBuilder, schema, multiFields, qsc, builder, allMetrics);
            if (composite == null) {
                return null;
            }
            builder.measure("count", List.of(), ScalarType.I64, COUNT_COLUMN);
            for (Metric metric : allMetrics) {
                metric.addMeasures(builder);
            }
            return new Plan(builder.build(), List.of(), composite, List.of(), allMetrics);
        }
        List<Level> levels = new ArrayList<>();
        long estimatedGroups = 1L;
        AggregationBuilder current = top.get(0);
        while (current != null) {
            ValuesSourceAggregationBuilder<?> bucketBuilder = (ValuesSourceAggregationBuilder<?>) current;
            Column column = resolveColumn(bucketBuilder.field(), schema, multiFields, qsc);
            if (column == null) {
                return null;
            }
            Expression keyExpression;
            KeyKind keyKind;
            long dateInterval = 0L;
            Rounding rounding = null;
            if (bucketBuilder instanceof TermsAggregationBuilder terms) {
                if (column.isUtf8()) {
                    keyExpression = new FieldReference(column.index());
                    keyKind = KeyKind.STRING;
                } else if (column.isFloating()) {
                    keyExpression = new FieldReference(column.index());
                    keyKind = KeyKind.DOUBLE;
                } else {
                    keyExpression = column.numericExpression();
                    keyKind = KeyKind.LONG;
                }
                // Every terms level multiplies the combinations the scan
                // may return by the groups this level keeps.
                estimatedGroups = saturatingMultiply(estimatedGroups, thresholds(terms).getShardSize());
                if (estimatedGroups > maxGroups) {
                    return null;
                }
            } else if (bucketBuilder instanceof HistogramAggregationBuilder histogram) {
                if (column.isUtf8() || column.isDate() || column.isBoolean()) {
                    return null;
                }
                keyExpression = SubstraitExpressions.floorFp64(
                    new FieldReference(column.index()),
                    histogram.offset(),
                    histogram.interval()
                );
                keyKind = KeyKind.LONG;
            } else {
                DateHistogramAggregationBuilder dateHistogram = (DateHistogramAggregationBuilder) bucketBuilder;
                if (!column.isDate()) {
                    return null;
                }
                String calendarUnit = LanceAggregationSupport.calendarUnit(dateHistogram);
                if (calendarUnit != null) {
                    // date_trunc takes a Timestamp array only (no Date32) and
                    // truncates in the column's zone, which has to be UTC to
                    // match the aggregator's rounding for a request without
                    // time_zone; other columns take the aggregator path. The
                    // key is the bucket start in millis, so no interval is
                    // multiplied back.
                    if (!column.isUtcTimestamp()) {
                        return null;
                    }
                    Expression truncated = SubstraitExpressions.dateTrunc(calendarUnit, new FieldReference(column.index()));
                    keyExpression = SubstraitExpressions.epochMillis(truncated, column.type());
                    rounding = Rounding.builder(
                        DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(dateHistogram.getCalendarInterval().toString())
                    ).build();
                } else {
                    dateInterval = fixedIntervalMillisOrZero(dateHistogram.getFixedInterval().toString());
                    if (dateInterval <= 0L) {
                        return null;
                    }
                    keyExpression = SubstraitExpressions.floorDivInt64(column.numericExpression(), dateHistogram.offset(), dateInterval);
                    rounding = Rounding.builder(TimeValue.timeValueMillis(dateInterval)).build();
                }
                keyKind = KeyKind.LONG;
            }
            List<Child> children = new ArrayList<>();
            List<Metric> metrics = new ArrayList<>();
            AggregationBuilder nested = null;
            for (AggregationBuilder sub : bucketBuilder.getSubAggregations()) {
                if (LanceAggregationSupport.isPushdownMetric(sub)) {
                    Metric metric = resolveMetric(sub, schema, multiFields, qsc, allMetrics);
                    if (metric == null) {
                        return null;
                    }
                    children.add(new Child(metric));
                    metrics.add(metric);
                } else {
                    nested = sub;
                    children.add(new Child(null));
                }
            }
            builder.groupBy(keyExpression, KEY_COLUMN_PREFIX + levels.size());
            DocValueFormat format = column.fieldType().docValueFormat(bucketBuilder.format(), bucketBuilder.timeZone());
            levels.add(new Level(bucketBuilder, column, keyKind, format, children, metrics, dateInterval, rounding));
            current = nested;
        }
        builder.measure("count", List.of(), ScalarType.I64, COUNT_COLUMN);
        for (Metric metric : allMetrics) {
            metric.addMeasures(builder);
        }
        return new Plan(builder.build(), levels, null, List.of(), allMetrics);
    }

    /**
     * Resolves a {@code composite}: one grouping per source, the sources'
     * formats and directions, the parsed {@code after} values and the
     * metric children. Returns null when a field does not resolve or an
     * {@code after} value is not of the type the source parses, in which
     * case the aggregator raises the request error.
     */
    private static Composite resolveComposite(
        CompositeAggregationBuilder compositeBuilder,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        SubstraitAggregatePlan.Builder builder,
        List<Metric> allMetrics
    ) {
        Map<String, Object> after = afterKey(compositeBuilder);
        List<Source> sources = new ArrayList<>(compositeBuilder.sources().size());
        for (CompositeValuesSourceBuilder<?> sourceBuilder : compositeBuilder.sources()) {
            Column column = resolveColumn(sourceBuilder.field(), schema, multiFields, qsc);
            if (column == null) {
                return null;
            }
            Expression keyExpression;
            KeyKind keyKind;
            DocValueFormat format;
            long dateInterval = 0L;
            if (sourceBuilder instanceof DateHistogramValuesSourceBuilder dateHistogram) {
                if (!column.isDate()) {
                    return null;
                }
                dateInterval = fixedIntervalMillisOrZero(dateHistogram.getIntervalAsFixed().toString());
                if (dateInterval <= 0L) {
                    return null;
                }
                keyExpression = SubstraitExpressions.floorDivInt64(column.numericExpression(), dateHistogram.offset(), dateInterval);
                keyKind = KeyKind.LONG;
                // The composite date source keeps the key as raw millis
                // unless the request names a format.
                format = sourceBuilder.format() == null
                    ? DocValueFormat.RAW
                    : column.fieldType().docValueFormat(sourceBuilder.format(), dateHistogram.timeZone());
            } else {
                if (column.isUtf8()) {
                    keyExpression = new FieldReference(column.index());
                    keyKind = KeyKind.STRING;
                } else if (column.isFloating()) {
                    keyExpression = new FieldReference(column.index());
                    keyKind = KeyKind.DOUBLE;
                } else {
                    keyExpression = column.numericExpression();
                    keyKind = KeyKind.LONG;
                }
                // A terms source on a date field also defaults to raw millis.
                format = sourceBuilder.format() == null && column.isDate()
                    ? DocValueFormat.RAW
                    : column.fieldType().docValueFormat(sourceBuilder.format(), null);
            }
            Comparable<?> afterValue = null;
            if (after != null) {
                afterValue = parseAfter(keyKind, format, after.get(sourceBuilder.name()));
                if (afterValue == null) {
                    return null;
                }
            }
            int reverseMul = sourceBuilder.order() == SortOrder.ASC ? 1 : -1;
            builder.groupBy(keyExpression, KEY_COLUMN_PREFIX + sources.size());
            sources.add(new Source(sourceBuilder, column, keyKind, format, reverseMul, afterValue, dateInterval));
        }
        List<Metric> metrics = resolveMetrics(new ArrayList<>(compositeBuilder.getSubAggregations()), schema, multiFields, qsc, allMetrics);
        if (metrics == null) {
            return null;
        }
        return new Composite(compositeBuilder, sources, metrics);
    }

    /**
     * The request's {@code after} map. The builder has a setter but no
     * getter for it, so it is read back from the builder's own JSON
     * rendering, which writes {@code after} under the {@code composite}
     * key only when the request carried one.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> afterKey(CompositeAggregationBuilder composite) {
        String json = Strings.toString(XContentType.JSON, composite);
        Map<String, Object> rendered = XContentHelper.convertToMap(new BytesArray(json), false, XContentType.JSON).v2();
        Map<String, Object> body = (Map<String, Object>) rendered.get(composite.getName());
        Map<String, Object> definition = (Map<String, Object>) body.get(CompositeAggregationBuilder.NAME);
        return (Map<String, Object>) definition.get(CompositeAggregationBuilder.AFTER_FIELD_NAME.getPreferredName());
    }

    /**
     * One {@code after} value parsed the way the source's values source
     * parses it: a string through the format for keyword sources, a
     * number or a formatted string for the numeric ones. Null when the
     * value has a type the aggregator would reject.
     */
    private static Comparable<?> parseAfter(KeyKind keyKind, DocValueFormat format, Object value) {
        if (value == null) {
            return null;
        }
        try {
            return switch (keyKind) {
                case STRING -> value instanceof String text ? format.parseBytesRef(text) : null;
                case LONG -> format.parseLong(value.toString(), false, NO_NOW_IN_AFTER);
                case DOUBLE -> value instanceof Number number
                    ? number.doubleValue()
                    : format.parseDouble(value.toString(), false, NO_NOW_IN_AFTER);
            };
        } catch (RuntimeException unparseable) {
            return null;
        }
    }

    private static List<Metric> resolveMetrics(
        List<AggregationBuilder> builders,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        List<Metric> allMetrics
    ) {
        List<Metric> metrics = new ArrayList<>(builders.size());
        for (AggregationBuilder builder : builders) {
            Metric metric = resolveMetric(builder, schema, multiFields, qsc, allMetrics);
            if (metric == null) {
                return null;
            }
            metrics.add(metric);
        }
        return metrics;
    }

    /** Resolves one metric and registers it in {@code allMetrics}, whose position is the metric's slot. */
    private static Metric resolveMetric(
        AggregationBuilder builder,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        List<Metric> allMetrics
    ) {
        ValuesSourceAggregationBuilder<?> source = (ValuesSourceAggregationBuilder<?>) builder;
        Column column = resolveColumn(source.field(), schema, multiFields, qsc);
        if (column == null) {
            return null;
        }
        MetricKind kind;
        if (builder instanceof SumAggregationBuilder) {
            kind = MetricKind.SUM;
        } else if (builder instanceof AvgAggregationBuilder) {
            kind = MetricKind.AVG;
        } else if (builder instanceof MinAggregationBuilder) {
            kind = MetricKind.MIN;
        } else if (builder instanceof MaxAggregationBuilder) {
            kind = MetricKind.MAX;
        } else if (builder instanceof ValueCountAggregationBuilder) {
            kind = MetricKind.VALUE_COUNT;
        } else {
            return null;
        }
        // Arithmetic metrics need a number; value_count only needs
        // the column to be single valued, which every accepted type is.
        if (kind != MetricKind.VALUE_COUNT && column.isUtf8()) {
            return null;
        }
        DocValueFormat format = column.fieldType().docValueFormat(source.format(), source.timeZone());
        Metric metric = new Metric(builder.getName(), kind, column, format, metadata(builder), allMetrics.size());
        allMetrics.add(metric);
        return metric;
    }

    /**
     * Maps an aggregation field to a Lance column. The field must be
     * mapped, name a top level column or a keyword sub-field of one,
     * and the mapping type must agree with the column's Arrow type.
     */
    private static Column resolveColumn(
        String field,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc
    ) {
        MappedFieldType fieldType = qsc.fieldMapper(field);
        if (fieldType == null) {
            return null;
        }
        String columnName = field;
        int dot = field.lastIndexOf('.');
        if (indexOf(schema, columnName) < 0 && dot > 0 && multiFields != null) {
            String base = field.substring(0, dot);
            String sub = field.substring(dot + 1);
            LinkedHashMap<String, String> subs = multiFields.get(base);
            if (subs == null || !"keyword".equals(subs.get(sub))) {
                return null;
            }
            columnName = base;
        }
        int index = indexOf(schema, columnName);
        if (index < 0) {
            return null;
        }
        ArrowType type = schema.getFields().get(index).getType();
        if (!typeMatches(fieldType.typeName(), type)) {
            return null;
        }
        return new Column(columnName, index, type, fieldType);
    }

    private static boolean typeMatches(String mappingType, ArrowType type) {
        return switch (mappingType) {
            case "keyword" -> type instanceof ArrowType.Utf8;
            case "byte", "short", "integer", "long" -> type instanceof ArrowType.Int intType && intType.getIsSigned();
            case "float" -> type instanceof ArrowType.FloatingPoint fp && fp.getPrecision() == FloatingPointPrecision.SINGLE;
            case "double" -> type instanceof ArrowType.FloatingPoint fp && fp.getPrecision() == FloatingPointPrecision.DOUBLE;
            case "boolean" -> type instanceof ArrowType.Bool;
            case "date" -> type instanceof ArrowType.Date || type instanceof ArrowType.Timestamp;
            default -> false;
        };
    }

    /**
     * The builder's {@code meta}, as the aggregator receives it: the
     * builder's getter substitutes an empty map for an absent
     * {@code meta}, while the aggregator keeps null and the response then
     * carries no {@code meta} key at all.
     */
    private static Map<String, Object> metadata(AggregationBuilder builder) {
        Map<String, Object> metadata = builder.getMetadata();
        return metadata == null || metadata.isEmpty() ? null : metadata;
    }

    private static int indexOf(Schema schema, String column) {
        List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(column)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The {@code terms} bucket thresholds as the aggregator factory
     * derives them: -1 is the builder's "not set" {@code shard_size}, for
     * which the factory applies the distributed counting heuristic on
     * non key orders.
     */
    private static TermsAggregator.BucketCountThresholds thresholds(TermsAggregationBuilder terms) {
        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(
            terms.minDocCount(),
            terms.shardMinDocCount(),
            terms.size(),
            terms.shardSize()
        );
        if (!InternalOrder.isKeyOrder(terms.order()) && thresholds.getShardSize() == -1) {
            thresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(thresholds.getRequiredSize()));
        }
        thresholds.ensureValidity();
        return thresholds;
    }

    private static long saturatingMultiply(long a, long b) {
        try {
            return Math.multiplyExact(a, b);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Milliseconds of a {@code fixed_interval}, parsed the way the
     * builders parse it; 0 when the text is not a time value.
     */
    private static long fixedIntervalMillisOrZero(String fixedInterval) {
        try {
            return TimeValue.parseTimeValue(fixedInterval, null, "fixed_interval").getMillis();
        } catch (IllegalArgumentException unparseable) {
            return 0L;
        }
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
     * The group key as the object the bucket type expects: {@link BytesRef},
     * {@link Long} or {@link Double}. A fixed interval {@code date_histogram}
     * key comes back as the interval quotient and is multiplied back to
     * millis ({@code dateInterval} 0 leaves the value as read).
     */
    private static Object key(FieldVector vector, int row, long dateInterval) {
        if (vector instanceof VarCharVector v) {
            return new BytesRef(v.get(row));
        }
        if (vector instanceof Float4Vector v) {
            return (double) v.get(row);
        }
        if (vector instanceof Float8Vector v) {
            return v.get(row);
        }
        long value = asLong(vector, row);
        return dateInterval > 0L ? value * dateInterval : value;
    }

    private static long longOrZero(FieldVector vector, int row) {
        return vector.isNull(row) ? 0L : asLong(vector, row);
    }

    private static double doubleOrZero(FieldVector vector, int row) {
        return doubleOr(vector, row, 0d);
    }

    private static double doubleOr(FieldVector vector, int row, double whenNull) {
        if (vector.isNull(row)) {
            return whenNull;
        }
        if (vector instanceof Float4Vector v) {
            return v.get(row);
        }
        if (vector instanceof Float8Vector v) {
            return v.get(row);
        }
        return (double) asLong(vector, row);
    }

    private static long asLong(FieldVector vector, int row) {
        if (vector instanceof BigIntVector v) {
            return v.get(row);
        }
        if (vector instanceof IntVector v) {
            return v.get(row);
        }
        if (vector instanceof SmallIntVector v) {
            return v.get(row);
        }
        if (vector instanceof TinyIntVector v) {
            return v.get(row);
        }
        if (vector instanceof BitVector v) {
            return v.get(row);
        }
        throw new IllegalStateException(
            "unexpected Arrow vector " + vector.getClass().getSimpleName() + " for aggregate column " + vector.getName()
        );
    }
}
