/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.PriorityQueue;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.common.Rounding;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.BitMixer;
import org.opensearch.common.util.Comparators;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.lance.dispatch.planner.AggregationRewriteContext;
import org.opensearch.lance.dispatch.planner.AggregationRewriteRegistry;
import org.opensearch.lance.dispatch.planner.PushdownPlan;
import org.opensearch.lance.engine.FragmentGroupScan;
import org.opensearch.lance.engine.LanceCancellation;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.BoolLiteral;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Cast;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Expression;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.FieldReference;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Float64Literal;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.Int64Literal;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.ScalarFunction;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.ScalarType;
import org.opensearch.lance.query.substrait.SubstraitAggregatePlan.StringLiteral;
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
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.filter.InternalFilters;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.aggregations.bucket.range.AbstractRangeBuilder;
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
import org.opensearch.search.aggregations.metrics.AbstractPercentilesAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalExtendedStats;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalStats;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentileRanks;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentileRanksAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesConfig;
import org.opensearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TDigestState;
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
public final class LanceAggregatePushdown {

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
     * Bins of a pushed down percentiles histogram, from
     * {@code lance.aggregation.percentiles_bins}; the plugin stores the
     * node setting here at start and every dynamic update after.
     */
    private static volatile int percentilesBins = LancePlugin.AGGREGATION_PERCENTILES_BINS_SETTING.getDefault(Settings.EMPTY);

    public static void setPercentilesBins(int bins) {
        percentilesBins = bins;
    }

    /**
     * How many times {@code shard_size} groups each scan of a single
     * level {@code terms} keeps, from
     * {@code lance.aggregation.pushdown_topk_slack}; the plugin stores
     * the node setting here at start and every dynamic update after.
     */
    private static volatile int topkSlack = LancePlugin.AGGREGATION_PUSHDOWN_TOPK_SLACK_SETTING.getDefault(Settings.EMPTY);

    public static void setTopkSlack(int slack) {
        topkSlack = slack;
    }

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
        VALUE_COUNT,
        STATS,
        EXTENDED_STATS,
        CARDINALITY,
        PERCENTILES,
        PERCENTILE_RANKS
    }

    private enum KeyKind {
        STRING,
        LONG,
        DOUBLE
    }

    /**
     * How a bucket level turns its key into buckets. The first three
     * open one bucket per distinct key; the mask kinds read the key as
     * the bit set of the ranges or filters the row falls in
     * ({@link SubstraitExpressions#matchMask}) and put the row into
     * every bucket whose bit is set.
     */
    private enum LevelKind {
        TERMS,
        HISTOGRAM,
        DATE_HISTOGRAM,
        RANGE,
        DATE_RANGE,
        FILTERS,
        FILTER,
        MISSING;

        boolean isMask() {
            return this == RANGE || this == DATE_RANGE || this == FILTERS || this == FILTER || this == MISSING;
        }
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
     * and {@code InternalAvg} carries both for the coordinator's merge);
     * {@code stats} and {@code extended_stats} take the count, sum, min,
     * max and (extended) sum of squares columns at once. A
     * {@code cardinality} has no measure: it adds the field as a grouping
     * column {@code prefix + "_d"}, so the scan returns one row per
     * distinct value (per bucket), and the executor hashes each into a
     * HyperLogLog++ sketch as the row is read. A {@code percentiles} /
     * {@code percentile_ranks} takes {@code min} and {@code max} in the
     * first scan and, in a second scan of its own, the row count per
     * equal width bin ({@code prefix + "_b"} and {@code prefix + "_bc"}),
     * which the executor feeds to a TDigest sketch. {@code slot} is the
     * metric's position in the plan's metric list and in every
     * {@link GroupState}.
     */
    private record Metric(String name, MetricKind kind, Column column, DocValueFormat format, Map<String, Object> metadata, int slot,
        ValuesSourceAggregationBuilder<?> builder) {

        String prefix() {
            return "m" + slot;
        }

        /** Output column of the distinct values a {@code cardinality} groups by. */
        String distinctColumn() {
            return prefix() + "_d";
        }

        /** Output columns of the percentiles bin ordinal and its row count. */
        String binColumn() {
            return prefix() + "_b";
        }

        String binCountColumn() {
            return prefix() + "_bc";
        }

        void addMeasures(SubstraitAggregatePlan.Builder builder) {
            Expression value = column.numericExpression();
            ScalarType type = column.isFloating() ? ScalarType.FP64 : ScalarType.I64;
            Expression reference = new FieldReference(column.index());
            switch (kind) {
                case SUM -> builder.measure("sum", List.of(value), type, prefix());
                case MIN -> builder.measure("min", List.of(value), type, prefix());
                case MAX -> builder.measure("max", List.of(value), type, prefix());
                case VALUE_COUNT -> builder.measure("count", List.of(reference), ScalarType.I64, prefix());
                case AVG -> {
                    builder.measure("sum", List.of(value), type, prefix() + "_s");
                    builder.measure("count", List.of(reference), ScalarType.I64, prefix() + "_c");
                }
                case STATS, EXTENDED_STATS -> {
                    builder.measure("count", List.of(reference), ScalarType.I64, prefix() + "_c");
                    builder.measure("sum", List.of(value), type, prefix() + "_s");
                    builder.measure("min", List.of(value), type, prefix() + "_mn");
                    builder.measure("max", List.of(value), type, prefix() + "_mx");
                    if (kind == MetricKind.EXTENDED_STATS) {
                        builder.measure("sum", List.of(SubstraitExpressions.square(value)), ScalarType.FP64, prefix() + "_q");
                    }
                }
                case PERCENTILES, PERCENTILE_RANKS -> {
                    // The bounds the bins of the second scan are cut from.
                    builder.measure("min", List.of(value), type, prefix() + "_mn");
                    builder.measure("max", List.of(value), type, prefix() + "_mx");
                }
                case CARDINALITY -> {
                    // No measure: the grouping on the field is added by
                    // the planner, and the distinct values come back as
                    // the group keys.
                }
            }
        }

        /** The grouping expression a {@code cardinality} adds: the value as the aggregator hashes it. */
        Expression distinctExpression() {
            return column.isUtf8() || column.isFloating() ? new FieldReference(column.index()) : column.numericExpression();
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
                case STATS, EXTENDED_STATS -> {
                    state.count = longOrZero(root.getVector(prefix() + "_c"), row);
                    state.sum = doubleOrZero(root.getVector(prefix() + "_s"), row);
                    state.min = doubleOr(root.getVector(prefix() + "_mn"), row, Double.POSITIVE_INFINITY);
                    state.max = doubleOr(root.getVector(prefix() + "_mx"), row, Double.NEGATIVE_INFINITY);
                    if (kind == MetricKind.EXTENDED_STATS) {
                        state.sumOfSquares = doubleOrZero(root.getVector(prefix() + "_q"), row);
                    }
                }
                case PERCENTILES, PERCENTILE_RANKS -> {
                    state.min = doubleOr(root.getVector(prefix() + "_mn"), row, Double.POSITIVE_INFINITY);
                    state.max = doubleOr(root.getVector(prefix() + "_mx"), row, Double.NEGATIVE_INFINITY);
                }
                case CARDINALITY -> {
                    FieldVector distinct = root.getVector(distinctColumn());
                    if (!distinct.isNull(row)) {
                        state.hash = hash(distinct, row);
                        state.hashed = true;
                        state.precision = precision();
                    }
                }
            }
            return state;
        }

        /**
         * The metric's values on one row of its bin scan: the rows of one
         * bin, spread over the bin as one value at each edge and the rest
         * at the centre. {@code min}, {@code max} and {@code width} are
         * the bounds the scan was planned with. The edges matter to
         * TDigest, which expects its lowest and highest centroids to be
         * single values (it reports them as the 0th and 100th percentile
         * and asserts it when it compresses): whatever bins a bucket's
         * rows occupy, its digest then starts and ends on a singleton.
         * Every value stays inside its bin, so the digest is accurate to
         * the bin width, and the edges of the outermost bins are the
         * exact minimum and maximum.
         */
        MetricState readBin(VectorSchemaRoot root, int row, double min, double max, double width, int bins) {
            MetricState state = new MetricState();
            FieldVector bin = root.getVector(binColumn());
            long count = longOrZero(root.getVector(binCountColumn()), row);
            if (bin.isNull(row) || count == 0L) {
                return state;
            }
            // The maximum value itself lands in bin `bins` (its quotient is
            // exactly bins); it belongs to the last bin.
            long ordinal = Math.max(0L, Math.min(asLong(bin, row), bins - 1L));
            double lower = ordinal == 0L ? min : min + ordinal * width;
            double upper = ordinal == bins - 1L || max == min ? max : Math.min(max, min + (ordinal + 1L) * width);
            double centre = (lower + upper) / 2d;
            if (count == 1L) {
                state.binValues = new double[] { centre };
                state.binWeights = new long[] { 1L };
            } else if (count == 2L) {
                state.binValues = new double[] { lower, upper };
                state.binWeights = new long[] { 1L, 1L };
            } else {
                state.binValues = new double[] { lower, centre, upper };
                state.binWeights = new long[] { 1L, count - 2L, 1L };
            }
            state.compression = compression();
            return state;
        }

        /**
         * The hash the cardinality aggregator computes for the value:
         * MurmurHash3 of the UTF-8 bytes for strings, the mixed bits for
         * numbers (a float as the double it widens to, a date as its
         * epoch millis, a boolean as 0 / 1).
         */
        private static long hash(FieldVector vector, int row) {
            if (vector instanceof VarCharVector v) {
                byte[] bytes = v.get(row);
                return MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128()).h1;
            }
            if (vector instanceof Float4Vector v) {
                return BitMixer.mix64(Double.doubleToLongBits(v.get(row)));
            }
            if (vector instanceof Float8Vector v) {
                return BitMixer.mix64(Double.doubleToLongBits(v.get(row)));
            }
            return BitMixer.mix64(asLong(vector, row));
        }

        /** HyperLogLog++ precision from the request's {@code precision_threshold}, as the aggregator factory derives it. */
        int precision() {
            Long threshold = precisionThreshold((CardinalityAggregationBuilder) builder);
            return threshold == null ? HyperLogLogPlusPlus.DEFAULT_PRECISION : HyperLogLogPlusPlus.precisionFromThreshold(threshold);
        }

        /** TDigest compression of a percentiles builder, the default 100 when the request names none. */
        double compression() {
            PercentilesConfig config = ((AbstractPercentilesAggregationBuilder<?>) builder).percentilesConfig();
            return config instanceof PercentilesConfig.TDigest tdigest
                ? tdigest.getCompression()
                : new PercentilesConfig.TDigest().getCompression();
        }

        /** The aggregation the aggregator would report for the merged values. */
        InternalAggregation toAggregation(MetricState state) {
            return switch (kind) {
                case SUM -> new InternalSum(name, state.sum, format, metadata);
                case MIN -> new InternalMin(name, state.min, format, metadata);
                case MAX -> new InternalMax(name, state.max, format, metadata);
                case VALUE_COUNT -> new InternalValueCount(name, state.count, metadata);
                case AVG -> new InternalAvg(name, state.sum, state.count, format, metadata);
                case STATS -> new InternalStats(name, state.count, state.sum, state.min, state.max, format, metadata);
                case EXTENDED_STATS -> new InternalExtendedStats(
                    name,
                    state.count,
                    state.sum,
                    state.min,
                    state.max,
                    state.sumOfSquares,
                    ((ExtendedStatsAggregationBuilder) builder).sigma(),
                    format,
                    metadata
                );
                case CARDINALITY -> {
                    // The aggregator reports null counts for a bucket
                    // without a value, not an empty sketch.
                    state.materialize();
                    HyperLogLogPlusPlus sketch = state.sketch != null && state.sketch.cardinality(0) > 0L ? state.sketch : null;
                    yield CoreAggregationResults.cardinality(name, sketch, metadata);
                }
                case PERCENTILES -> {
                    state.materialize();
                    yield new InternalTDigestPercentiles(
                        name,
                        ((PercentilesAggregationBuilder) builder).percentiles(),
                        state.digest != null ? state.digest : new TDigestState(compression()),
                        ((PercentilesAggregationBuilder) builder).keyed(),
                        format,
                        metadata
                    );
                }
                case PERCENTILE_RANKS -> {
                    state.materialize();
                    yield new InternalTDigestPercentileRanks(
                        name,
                        ((PercentileRanksAggregationBuilder) builder).values(),
                        state.digest != null ? state.digest : new TDigestState(compression()),
                        ((PercentileRanksAggregationBuilder) builder).keyed(),
                        format,
                        metadata
                    );
                }
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

        boolean isPercentiles() {
            return kind == MetricKind.PERCENTILES || kind == MetricKind.PERCENTILE_RANKS;
        }
    }

    /**
     * Running values of one metric over the rows of one group. A fresh
     * state holds the neutral element of every measure (0 for sums and
     * counts, the infinities for min and max, no sketch), which is also
     * what the aggregator reports for a bucket without documents, so
     * merging a state into a fresh one copies it and merging two partial
     * states adds the sums and counts and keeps the smaller min and
     * larger max. The sketches are lazy: a row state carries one hash
     * (cardinality) or one bin (percentiles), the first merge into a
     * group state opens the sketch and inserts it, and merging two group
     * states merges the sketches. A sketch is copied before it is merged
     * into a fresh state, so the states of the scan's groups can be
     * folded into an outer bucket and still feed the nested level.
     */
    private static final class MetricState {
        double sum;
        long count;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sumOfSquares;

        /** Cardinality: the hash of one row's value, and the sketch of a group. */
        long hash;
        boolean hashed;
        int precision;
        HyperLogLogPlusPlus sketch;

        /** Percentiles: the values and weights of one bin, and the digest of a group. */
        double[] binValues;
        long[] binWeights;
        double compression;
        TDigestState digest;

        void merge(MetricState other) {
            sum += other.sum;
            count += other.count;
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sumOfSquares += other.sumOfSquares;
            // This state may still be the single row it was read as;
            // its own value goes into the sketch before the other's.
            materialize();
            if (other.sketch != null) {
                if (sketch == null) {
                    sketch = new HyperLogLogPlusPlus(other.sketch.precision(), BigArrays.NON_RECYCLING_INSTANCE, 1);
                }
                sketch.merge(0, other.sketch, 0);
            } else if (other.hashed) {
                collect(other.precision, other.hash);
            }
            if (other.digest != null) {
                if (digest == null) {
                    digest = new TDigestState(other.digest.compression());
                }
                digest.add(other.digest);
            } else if (other.binValues != null) {
                addBin(other.compression, other.binValues, other.binWeights);
            }
        }

        /**
         * Moves a row's own hash or bin into this state's sketch. A state
         * read from one row keeps the value itself until it is merged
         * with another or reported, so the scan does not allocate a
         * sketch per row.
         */
        void materialize() {
            if (hashed) {
                hashed = false;
                collect(precision, hash);
            }
            if (binValues != null) {
                double[] values = binValues;
                long[] weights = binWeights;
                binValues = null;
                binWeights = null;
                addBin(compression, values, weights);
            }
        }

        private void collect(int precision, long hash) {
            if (sketch == null) {
                sketch = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
            }
            sketch.collect(0, hash);
        }

        private void addBin(double compression, double[] values, long[] weights) {
            if (digest == null) {
                digest = new TDigestState(compression);
            }
            for (int i = 0; i < values.length; i++) {
                // TDigest weights are ints; a bin of more rows is added in
                // slices.
                long remaining = weights[i];
                while (remaining > 0L) {
                    int slice = (int) Math.min(remaining, Integer.MAX_VALUE);
                    digest.add(values[i], slice);
                    remaining -= slice;
                }
            }
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
     * What one scan returned, and the merge of several. {@code total}
     * is the row count over every group, null keyed rows included. A
     * plan without groupings keeps its single row in
     * {@code metricsOnly} ({@code null} until a row arrived). A plan
     * with groupings keeps one state per key list in the primitive
     * columns of {@code groups}; the single level {@code terms} shape
     * ordered by {@code _count} or by one metric keeps the bounded per
     * scan selection in {@code topK} instead, with the row count over
     * every keyed group in {@code keyedCount}
     * ({@link Plan#mergePartials} folds the top-k partials into a
     * {@code groups} table before the buckets are built).
     */
    private static final class Partial {
        long total;
        long keyedCount;
        GroupState metricsOnly;
        GroupTable groups;
        TopKGroups topK;

        void merge(Partial other) {
            total += other.total;
            keyedCount += other.keyedCount;
            if (other.metricsOnly != null) {
                metricsOnly = metricsOnly == null ? other.metricsOnly : metricsOnly.merge(other.metricsOnly);
            }
            if (other.groups != null) {
                if (groups == null) {
                    groups = other.groups;
                } else {
                    groups.mergeFrom(other.groups);
                }
            }
        }
    }

    /**
     * Columnar running values of every metric of the plan over the
     * groups of a {@link GroupTable} or {@link TopKGroups}: one
     * primitive array per measure per metric slot, indexed by group, so
     * a scan reading millions of group rows writes into arrays instead
     * of allocating a {@link MetricState} per row and metric. The
     * sketches (the HyperLogLog++ of a {@code cardinality}, the TDigest
     * a percentiles bin scan feeds) stay one object per group, created
     * on the group's first value; their group count is bounded by the
     * plan's group estimate, not by the rows read. {@link #box} turns
     * one group's values back into the {@link MetricState} the bucket
     * assembly folds and reports.
     */
    private static final class MetricStore {
        private final List<Metric> metrics;
        private final double[][] sums;
        private final long[][] counts;
        private final double[][] mins;
        private final double[][] maxes;
        private final double[][] squares;
        private final HyperLogLogPlusPlus[][] sketches;
        private final TDigestState[][] digests;
        private int capacity;

        MetricStore(List<Metric> metrics, int initialCapacity) {
            this.metrics = metrics;
            this.capacity = initialCapacity;
            int slots = metrics.size();
            sums = new double[slots][];
            counts = new long[slots][];
            mins = new double[slots][];
            maxes = new double[slots][];
            squares = new double[slots][];
            sketches = new HyperLogLogPlusPlus[slots][];
            digests = new TDigestState[slots][];
            for (Metric metric : metrics) {
                int slot = metric.slot();
                switch (metric.kind()) {
                    case SUM -> sums[slot] = new double[capacity];
                    case MIN -> mins[slot] = filled(new double[capacity], 0, Double.POSITIVE_INFINITY);
                    case MAX -> maxes[slot] = filled(new double[capacity], 0, Double.NEGATIVE_INFINITY);
                    case VALUE_COUNT -> counts[slot] = new long[capacity];
                    case AVG -> {
                        sums[slot] = new double[capacity];
                        counts[slot] = new long[capacity];
                    }
                    case STATS, EXTENDED_STATS -> {
                        counts[slot] = new long[capacity];
                        sums[slot] = new double[capacity];
                        mins[slot] = filled(new double[capacity], 0, Double.POSITIVE_INFINITY);
                        maxes[slot] = filled(new double[capacity], 0, Double.NEGATIVE_INFINITY);
                        if (metric.kind() == MetricKind.EXTENDED_STATS) {
                            squares[slot] = new double[capacity];
                        }
                    }
                    case CARDINALITY -> sketches[slot] = new HyperLogLogPlusPlus[capacity];
                    case PERCENTILES, PERCENTILE_RANKS -> {
                        mins[slot] = filled(new double[capacity], 0, Double.POSITIVE_INFINITY);
                        maxes[slot] = filled(new double[capacity], 0, Double.NEGATIVE_INFINITY);
                        digests[slot] = new TDigestState[capacity];
                    }
                }
            }
        }

        private static double[] filled(double[] array, int from, double value) {
            Arrays.fill(array, from, array.length, value);
            return array;
        }

        void ensureCapacity(int needed) {
            if (needed <= capacity) {
                return;
            }
            int grown = Math.max(needed, capacity * 2);
            for (int slot = 0; slot < metrics.size(); slot++) {
                if (sums[slot] != null) {
                    sums[slot] = Arrays.copyOf(sums[slot], grown);
                }
                if (counts[slot] != null) {
                    counts[slot] = Arrays.copyOf(counts[slot], grown);
                }
                if (mins[slot] != null) {
                    mins[slot] = filled(Arrays.copyOf(mins[slot], grown), capacity, Double.POSITIVE_INFINITY);
                }
                if (maxes[slot] != null) {
                    maxes[slot] = filled(Arrays.copyOf(maxes[slot], grown), capacity, Double.NEGATIVE_INFINITY);
                }
                if (squares[slot] != null) {
                    squares[slot] = Arrays.copyOf(squares[slot], grown);
                }
                if (sketches[slot] != null) {
                    sketches[slot] = Arrays.copyOf(sketches[slot], grown);
                }
                if (digests[slot] != null) {
                    digests[slot] = Arrays.copyOf(digests[slot], grown);
                }
            }
            capacity = grown;
        }

        /** Back to the neutral element at {@code idx}, for a reused top-k slot. */
        void reset(int idx) {
            for (int slot = 0; slot < metrics.size(); slot++) {
                if (sums[slot] != null) {
                    sums[slot][idx] = 0d;
                }
                if (counts[slot] != null) {
                    counts[slot][idx] = 0L;
                }
                if (mins[slot] != null) {
                    mins[slot][idx] = Double.POSITIVE_INFINITY;
                }
                if (maxes[slot] != null) {
                    maxes[slot][idx] = Double.NEGATIVE_INFINITY;
                }
                if (squares[slot] != null) {
                    squares[slot][idx] = 0d;
                }
                if (sketches[slot] != null) {
                    sketches[slot][idx] = null;
                }
                if (digests[slot] != null) {
                    digests[slot][idx] = null;
                }
            }
        }

        void addSum(int slot, int idx, double value) {
            sums[slot][idx] += value;
        }

        void addCount(int slot, int idx, long value) {
            counts[slot][idx] += value;
        }

        void addMin(int slot, int idx, double value) {
            if (value < mins[slot][idx]) {
                mins[slot][idx] = value;
            }
        }

        void addMax(int slot, int idx, double value) {
            if (value > maxes[slot][idx]) {
                maxes[slot][idx] = value;
            }
        }

        void addSquare(int slot, int idx, double value) {
            squares[slot][idx] += value;
        }

        void collectHash(int slot, int idx, long hash, int precision) {
            HyperLogLogPlusPlus sketch = sketches[slot][idx];
            if (sketch == null) {
                sketch = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
                sketches[slot][idx] = sketch;
            }
            sketch.collect(0, hash);
        }

        /**
         * One bin scan row into the group's digest: the bin's rows as
         * one value at each bin edge and the rest at the centre, the
         * spread {@link Metric#readBin} documents.
         */
        void addBin(int slot, int idx, double compression, double min, double max, double width, int bins, long ordinal, long count) {
            TDigestState digest = digests[slot][idx];
            if (digest == null) {
                digest = new TDigestState(compression);
                digests[slot][idx] = digest;
            }
            long bin = Math.max(0L, Math.min(ordinal, bins - 1L));
            double lower = bin == 0L ? min : min + bin * width;
            double upper = bin == bins - 1L || max == min ? max : Math.min(max, min + (bin + 1L) * width);
            double centre = (lower + upper) / 2d;
            if (count == 1L) {
                digest.add(centre, 1);
                return;
            }
            digest.add(lower, 1);
            long remaining = count - 2L;
            while (remaining > 0L) {
                int slice = (int) Math.min(remaining, Integer.MAX_VALUE);
                digest.add(centre, slice);
                remaining -= slice;
            }
            digest.add(upper, 1);
        }

        void merge(int destIdx, MetricStore src, int srcIdx) {
            for (Metric metric : metrics) {
                int slot = metric.slot();
                if (src.sums[slot] != null) {
                    sums[slot][destIdx] += src.sums[slot][srcIdx];
                }
                if (src.counts[slot] != null) {
                    counts[slot][destIdx] += src.counts[slot][srcIdx];
                }
                if (src.mins[slot] != null) {
                    addMin(slot, destIdx, src.mins[slot][srcIdx]);
                }
                if (src.maxes[slot] != null) {
                    addMax(slot, destIdx, src.maxes[slot][srcIdx]);
                }
                if (src.squares[slot] != null) {
                    squares[slot][destIdx] += src.squares[slot][srcIdx];
                }
                if (src.sketches[slot] != null && src.sketches[slot][srcIdx] != null) {
                    HyperLogLogPlusPlus other = src.sketches[slot][srcIdx];
                    HyperLogLogPlusPlus sketch = sketches[slot][destIdx];
                    if (sketch == null) {
                        sketch = new HyperLogLogPlusPlus(other.precision(), BigArrays.NON_RECYCLING_INSTANCE, 1);
                        sketches[slot][destIdx] = sketch;
                    }
                    sketch.merge(0, other, 0);
                }
                if (src.digests[slot] != null && src.digests[slot][srcIdx] != null) {
                    TDigestState other = src.digests[slot][srcIdx];
                    TDigestState digest = digests[slot][destIdx];
                    if (digest == null) {
                        digest = new TDigestState(other.compression());
                        digests[slot][destIdx] = digest;
                    }
                    digest.add(other);
                }
            }
        }

        /**
         * The value of a single value metric at {@code idx}, as the
         * order comparator reads it from the built aggregation: the
         * sum, the min / max (an infinity when no row had a value), the
         * value count, or the average ({@code NaN} over a count of 0,
         * which sorts last in either direction).
         */
        double sortValue(int slot, int idx) {
            return switch (metrics.get(slot).kind()) {
                case SUM -> sums[slot][idx];
                case MIN -> mins[slot][idx];
                case MAX -> maxes[slot][idx];
                case VALUE_COUNT -> (double) counts[slot][idx];
                case AVG -> sums[slot][idx] / counts[slot][idx];
                default -> throw new IllegalStateException("not a single value metric: " + metrics.get(slot).kind());
            };
        }

        /** The smallest group minimum of {@code slot} over the first {@code size} groups, for the percentiles bin bounds. */
        double minOf(int slot, int size) {
            double min = Double.POSITIVE_INFINITY;
            for (int idx = 0; idx < size; idx++) {
                min = Math.min(min, mins[slot][idx]);
            }
            return min;
        }

        double maxOf(int slot, int size) {
            double max = Double.NEGATIVE_INFINITY;
            for (int idx = 0; idx < size; idx++) {
                max = Math.max(max, maxes[slot][idx]);
            }
            return max;
        }

        /** One group's values as the states the bucket assembly reads, by slot. */
        MetricState[] boxAll(int idx) {
            MetricState[] states = new MetricState[metrics.size()];
            for (int slot = 0; slot < states.length; slot++) {
                states[slot] = box(slot, idx);
            }
            return states;
        }

        private MetricState box(int slot, int idx) {
            MetricState state = new MetricState();
            switch (metrics.get(slot).kind()) {
                case SUM -> state.sum = sums[slot][idx];
                case MIN -> state.min = mins[slot][idx];
                case MAX -> state.max = maxes[slot][idx];
                case VALUE_COUNT -> state.count = counts[slot][idx];
                case AVG -> {
                    state.sum = sums[slot][idx];
                    state.count = counts[slot][idx];
                }
                case STATS, EXTENDED_STATS -> {
                    state.count = counts[slot][idx];
                    state.sum = sums[slot][idx];
                    state.min = mins[slot][idx];
                    state.max = maxes[slot][idx];
                    if (squares[slot] != null) {
                        state.sumOfSquares = squares[slot][idx];
                    }
                }
                case CARDINALITY -> state.sketch = sketches[slot][idx];
                case PERCENTILES, PERCENTILE_RANKS -> {
                    state.min = mins[slot][idx];
                    state.max = maxes[slot][idx];
                    state.digest = digests[slot][idx];
                }
            }
            return state;
        }
    }

    /**
     * The output vectors of one metric in one batch of a main scan,
     * resolved once per batch so the row loop reads primitives only:
     * the columnar counterpart of {@link Metric#read}.
     */
    private static final class MetricBatch {
        private final Metric metric;
        private final int slot;
        private final FieldVector value;
        private final FieldVector count;
        private final FieldVector min;
        private final FieldVector max;
        private final FieldVector square;
        private final FieldVector distinct;
        private final int precision;
        private final MurmurHash3.Hash128 hashScratch;
        private final BytesRefBuilder bytesScratch;

        MetricBatch(Metric metric, VectorSchemaRoot root) {
            this.metric = metric;
            this.slot = metric.slot();
            String prefix = metric.prefix();
            FieldVector value = null;
            FieldVector count = null;
            FieldVector min = null;
            FieldVector max = null;
            FieldVector square = null;
            FieldVector distinct = null;
            int precision = 0;
            switch (metric.kind()) {
                case SUM, MIN, MAX, VALUE_COUNT -> value = root.getVector(prefix);
                case AVG -> {
                    value = root.getVector(prefix + "_s");
                    count = root.getVector(prefix + "_c");
                }
                case STATS, EXTENDED_STATS -> {
                    count = root.getVector(prefix + "_c");
                    value = root.getVector(prefix + "_s");
                    min = root.getVector(prefix + "_mn");
                    max = root.getVector(prefix + "_mx");
                    if (metric.kind() == MetricKind.EXTENDED_STATS) {
                        square = root.getVector(prefix + "_q");
                    }
                }
                case CARDINALITY -> {
                    distinct = root.getVector(metric.distinctColumn());
                    precision = metric.precision();
                }
                case PERCENTILES, PERCENTILE_RANKS -> {
                    min = root.getVector(prefix + "_mn");
                    max = root.getVector(prefix + "_mx");
                }
            }
            this.value = value;
            this.count = count;
            this.min = min;
            this.max = max;
            this.square = square;
            this.distinct = distinct;
            this.precision = precision;
            this.hashScratch = distinct instanceof VarCharVector ? new MurmurHash3.Hash128() : null;
            this.bytesScratch = distinct instanceof VarCharVector ? new BytesRefBuilder() : null;
        }

        static MetricBatch[] resolve(List<Metric> metrics, VectorSchemaRoot root) {
            MetricBatch[] batches = new MetricBatch[metrics.size()];
            for (int i = 0; i < batches.length; i++) {
                batches[i] = new MetricBatch(metrics.get(i), root);
            }
            return batches;
        }

        /** The metric's values on one group row, into the group's columns. */
        void add(MetricStore store, int idx, int row) {
            switch (metric.kind()) {
                case SUM -> store.addSum(slot, idx, doubleOrZero(value, row));
                case MIN -> store.addMin(slot, idx, doubleOr(value, row, Double.POSITIVE_INFINITY));
                case MAX -> store.addMax(slot, idx, doubleOr(value, row, Double.NEGATIVE_INFINITY));
                case VALUE_COUNT -> store.addCount(slot, idx, longOrZero(value, row));
                case AVG -> {
                    store.addSum(slot, idx, doubleOrZero(value, row));
                    store.addCount(slot, idx, longOrZero(count, row));
                }
                case STATS, EXTENDED_STATS -> {
                    store.addCount(slot, idx, longOrZero(count, row));
                    store.addSum(slot, idx, doubleOrZero(value, row));
                    store.addMin(slot, idx, doubleOr(min, row, Double.POSITIVE_INFINITY));
                    store.addMax(slot, idx, doubleOr(max, row, Double.NEGATIVE_INFINITY));
                    if (square != null) {
                        store.addSquare(slot, idx, doubleOrZero(square, row));
                    }
                }
                case CARDINALITY -> {
                    if (!distinct.isNull(row)) {
                        store.collectHash(slot, idx, hash(row), precision);
                    }
                }
                case PERCENTILES, PERCENTILE_RANKS -> {
                    store.addMin(slot, idx, doubleOr(min, row, Double.POSITIVE_INFINITY));
                    store.addMax(slot, idx, doubleOr(max, row, Double.NEGATIVE_INFINITY));
                }
            }
        }

        /**
         * The hash the cardinality aggregator computes for the row's
         * distinct value, without the per row copies of
         * {@link Metric#hash}: the string bytes are read into a reused
         * scratch.
         */
        private long hash(int row) {
            if (distinct instanceof VarCharVector v) {
                BytesRef bytes = readUtf8(v, row, bytesScratch);
                return MurmurHash3.hash128(bytes.bytes, bytes.offset, bytes.length, 0, hashScratch).h1;
            }
            if (distinct instanceof Float4Vector v) {
                return BitMixer.mix64(Double.doubleToLongBits(v.get(row)));
            }
            if (distinct instanceof Float8Vector v) {
                return BitMixer.mix64(Double.doubleToLongBits(v.get(row)));
            }
            return BitMixer.mix64(asLong(distinct, row));
        }

        /** The row's value under {@link MetricStore#sortValue}, for the top-k test before the row is retained. */
        double rowSortValue(int row) {
            return switch (metric.kind()) {
                case SUM -> doubleOrZero(value, row);
                case MIN -> doubleOr(value, row, Double.POSITIVE_INFINITY);
                case MAX -> doubleOr(value, row, Double.NEGATIVE_INFINITY);
                case VALUE_COUNT -> (double) longOrZero(value, row);
                case AVG -> doubleOrZero(value, row) / longOrZero(count, row);
                default -> throw new IllegalStateException("not a single value metric: " + metric.kind());
            };
        }
    }

    /**
     * The groups of one scan (and the merge of several), keyed by the
     * full key list, in primitive columns: one {@code long} per key and
     * group (the value of an integer or date key, the
     * {@link Double#doubleToLongBits} of a floating point key, the id
     * of a string key in {@code dict}), a null bit set, the row counts,
     * and the metric columns of a {@link MetricStore}. Group identity
     * is an open addressing hash over the encoded keys, so merging a
     * row allocates nothing; the string dictionary is a
     * {@link BytesRefHash}, whose pooled bytes outlive the Arrow batch
     * the key was read from. {@link #box} converts the groups into the
     * {@link Group} list the bucket assembly folds, once, after every
     * partial is merged.
     */
    private static final class GroupTable {

        /** Ordering of two groups by index, for {@link #selectTop}. */
        interface GroupOrder {
            int compare(int a, int b);
        }

        private static final int INITIAL_CAPACITY = 16;

        private final KeyKind[] kinds;
        private final long[][] keys;
        private long[] nullBits;
        private long[] counts;
        final MetricStore metrics;
        private BytesRefHash dict;
        private final BytesRef spare = new BytesRef();
        private final BytesRef otherSpare = new BytesRef();
        private int[] slots;
        private int slotMask;
        private int size;

        GroupTable(KeyKind[] kinds, List<Metric> metrics) {
            this.kinds = kinds;
            this.keys = new long[kinds.length][];
            for (int k = 0; k < kinds.length; k++) {
                keys[k] = new long[INITIAL_CAPACITY];
            }
            this.nullBits = new long[INITIAL_CAPACITY];
            this.counts = new long[INITIAL_CAPACITY];
            this.metrics = new MetricStore(metrics, INITIAL_CAPACITY);
            this.slots = new int[INITIAL_CAPACITY * 2];
            this.slotMask = slots.length - 1;
        }

        int size() {
            return size;
        }

        /** The dictionary id of {@code bytes}, adding it when new. */
        long intern(BytesRef bytes) {
            if (dict == null) {
                dict = new BytesRefHash();
            }
            int id = dict.add(bytes);
            return id < 0 ? -id - 1 : id;
        }

        /** The group's index, adding an empty group when the key list is new. */
        int findOrAdd(long[] encoded, long nulls) {
            int slot = (int) (hashOf(encoded, nulls) & slotMask);
            while (true) {
                int entry = slots[slot];
                if (entry == 0) {
                    return add(encoded, nulls, slot);
                }
                int idx = entry - 1;
                if (nullBits[idx] == nulls && keysEqual(idx, encoded)) {
                    return idx;
                }
                slot = (slot + 1) & slotMask;
            }
        }

        private int add(long[] encoded, long nulls, int slot) {
            if (size == counts.length) {
                grow();
                // The slot table was rebuilt; probe again for the
                // insertion point.
                slot = (int) (hashOf(encoded, nulls) & slotMask);
                while (slots[slot] != 0) {
                    slot = (slot + 1) & slotMask;
                }
            }
            int idx = size++;
            for (int k = 0; k < kinds.length; k++) {
                keys[k][idx] = encoded[k];
            }
            nullBits[idx] = nulls;
            slots[slot] = idx + 1;
            return idx;
        }

        private void grow() {
            int grown = counts.length * 2;
            for (int k = 0; k < kinds.length; k++) {
                keys[k] = Arrays.copyOf(keys[k], grown);
            }
            nullBits = Arrays.copyOf(nullBits, grown);
            counts = Arrays.copyOf(counts, grown);
            metrics.ensureCapacity(grown);
            slots = new int[grown * 2];
            slotMask = slots.length - 1;
            for (int idx = 0; idx < size; idx++) {
                int slot = (int) (hashAt(idx) & slotMask);
                while (slots[slot] != 0) {
                    slot = (slot + 1) & slotMask;
                }
                slots[slot] = idx + 1;
            }
        }

        private long hashOf(long[] encoded, long nulls) {
            long hash = BitMixer.mix64(nulls);
            for (long value : encoded) {
                hash = BitMixer.mix64(hash ^ value);
            }
            return hash;
        }

        private long hashAt(int idx) {
            long hash = BitMixer.mix64(nullBits[idx]);
            for (int k = 0; k < kinds.length; k++) {
                hash = BitMixer.mix64(hash ^ keys[k][idx]);
            }
            return hash;
        }

        private boolean keysEqual(int idx, long[] encoded) {
            for (int k = 0; k < kinds.length; k++) {
                if (keys[k][idx] != encoded[k]) {
                    return false;
                }
            }
            return true;
        }

        void addCount(int idx, long count) {
            counts[idx] += count;
        }

        long countAt(int idx) {
            return counts[idx];
        }

        /** Sum of the counts of every group, for {@code sum_other_doc_count} under a key order. */
        long totalCount() {
            long total = 0L;
            for (int idx = 0; idx < size; idx++) {
                total += counts[idx];
            }
            return total;
        }

        void mergeFrom(GroupTable src) {
            long[] encoded = new long[kinds.length];
            for (int g = 0; g < src.size; g++) {
                for (int k = 0; k < kinds.length; k++) {
                    if (kinds[k] == KeyKind.STRING && (src.nullBits[g] & (1L << k)) == 0L) {
                        src.dict.get((int) src.keys[k][g], src.spare);
                        encoded[k] = intern(src.spare);
                    } else {
                        encoded[k] = src.keys[k][g];
                    }
                }
                int idx = findOrAdd(encoded, src.nullBits[g]);
                counts[idx] += src.counts[g];
                metrics.merge(idx, src.metrics, g);
            }
        }

        /** Key order of the first key column, ascending, the terms tie break. */
        int compareKeys(int a, int b) {
            return switch (kinds[0]) {
                case LONG -> Long.compare(keys[0][a], keys[0][b]);
                case DOUBLE -> Double.compare(Double.longBitsToDouble(keys[0][a]), Double.longBitsToDouble(keys[0][b]));
                case STRING -> {
                    dict.get((int) keys[0][a], spare);
                    dict.get((int) keys[0][b], otherSpare);
                    yield spare.compareTo(otherSpare);
                }
            };
        }

        /**
         * The indices of the first {@code n} groups by {@code order},
         * best first: a bounded heap over every group, the primitive
         * counterpart of the terms selection queue.
         */
        int[] selectTop(int n, GroupOrder order) {
            int keep = Math.min(n, size);
            int[] heap = new int[keep];
            int heapSize = 0;
            for (int idx = 0; idx < size; idx++) {
                if (heapSize < keep) {
                    heap[heapSize] = idx;
                    siftUp(heap, heapSize++, order);
                } else if (keep > 0 && order.compare(idx, heap[0]) < 0) {
                    heap[0] = idx;
                    siftDown(heap, heapSize, 0, order);
                }
            }
            int[] selected = new int[heapSize];
            for (int i = heapSize - 1; i >= 0; i--) {
                selected[i] = heap[0];
                heap[0] = heap[--heapSize];
                siftDown(heap, heapSize, 0, order);
            }
            return selected;
        }

        /** The heap's top is the worst retained group: a parent sorts after its children. */
        private static void siftUp(int[] heap, int pos, GroupOrder order) {
            while (pos > 0) {
                int parent = (pos - 1) / 2;
                if (order.compare(heap[pos], heap[parent]) <= 0) {
                    return;
                }
                int swap = heap[pos];
                heap[pos] = heap[parent];
                heap[parent] = swap;
                pos = parent;
            }
        }

        private static void siftDown(int[] heap, int heapSize, int pos, GroupOrder order) {
            while (true) {
                int worst = pos;
                int left = pos * 2 + 1;
                int right = left + 1;
                if (left < heapSize && order.compare(heap[left], heap[worst]) > 0) {
                    worst = left;
                }
                if (right < heapSize && order.compare(heap[right], heap[worst]) > 0) {
                    worst = right;
                }
                if (worst == pos) {
                    return;
                }
                int swap = heap[pos];
                heap[pos] = heap[worst];
                heap[worst] = swap;
                pos = worst;
            }
        }

        /** One group's key at position {@code k} as the object the bucket types expect. */
        Object boxKey(int k, int idx) {
            if ((nullBits[idx] & (1L << k)) != 0L) {
                return null;
            }
            return switch (kinds[k]) {
                case LONG -> keys[k][idx];
                case DOUBLE -> Double.longBitsToDouble(keys[k][idx]);
                case STRING -> {
                    dict.get((int) keys[k][idx], spare);
                    yield BytesRef.deepCopyOf(spare);
                }
            };
        }

        /** Every group as the boxed form the nested and composite assembly reads. */
        List<Group> box() {
            List<Group> groups = new ArrayList<>(size);
            for (int idx = 0; idx < size; idx++) {
                Object[] boxedKeys = new Object[kinds.length];
                for (int k = 0; k < kinds.length; k++) {
                    boxedKeys[k] = boxKey(k, idx);
                }
                groups.add(new Group(Arrays.asList(boxedKeys), new GroupState(counts[idx], metrics.boxAll(idx))));
            }
            return groups;
        }
    }

    /**
     * The bounded selection of one scan of the single level
     * {@code terms} shape ordered by {@code _count} or by one single
     * value metric: at most {@code limit} groups ({@code shard_size}
     * times {@code lance.aggregation.pushdown_topk_slack}) are kept in
     * a primitive heap whose top is the weakest retained group under
     * the request order (count descending or the metric in its
     * direction, key ascending as the tie breaker); a row that does not
     * beat it only adds its count to {@code keyedCount}, which
     * {@code sum_other_doc_count} is later computed from, so the scan's
     * memory and allocations stop growing at {@code limit} groups
     * however many groups Lance returns.
     *
     * <p>Entries are not coalesced here: Lance runs the aggregate to
     * completion before emitting rows, so one scan returns each group
     * at most once. The same key retained by several fragment group
     * scans is summed when {@link Plan#mergePartials} folds the
     * per scan selections into one {@link GroupTable} and the final
     * {@code shard_size} cut re-evaluates the summed counts. A key a
     * scan dropped loses that scan's rows the way a term a shard did
     * not return loses that shard's: the doc count error the reduce
     * derives from the smallest returned bucket keeps its meaning, and
     * a slack large enough to retain every group makes the result
     * exact.
     */
    private static final class TopKGroups {
        private final KeyKind keyKind;
        private final int sortSlot;
        private final boolean sortAsc;
        private final int limit;
        final MetricStore metrics;
        private long[] keys;
        private long[] counts;
        private double[] sortValues;
        private BytesRefHash dict;
        private final BytesRef spare = new BytesRef();
        private final BytesRef otherSpare = new BytesRef();
        private int[] heap;
        private int size;
        private long keyedCount;

        TopKGroups(TopKSpec spec, List<Metric> allMetrics) {
            this.keyKind = spec.keyKind();
            this.sortSlot = spec.sortSlot();
            this.sortAsc = spec.sortAsc();
            this.limit = spec.perScanLimit();
            int initial = Math.min(limit, 16);
            this.keys = new long[initial];
            this.counts = new long[initial];
            this.sortValues = sortSlot >= 0 ? new double[initial] : null;
            this.metrics = new MetricStore(allMetrics, initial);
            this.heap = new int[initial];
        }

        long keyedCount() {
            return keyedCount;
        }

        /**
         * One group row: retained when the selection has room or the
         * row beats the weakest retained group. Returns the entry index
         * the caller writes the row's metrics into, or -1 when the row
         * only counts toward {@code keyedCount}.
         */
        int offer(long numericKey, BytesRef stringKey, long count, double sortValue) {
            keyedCount += count;
            if (size < limit) {
                if (size == counts.length) {
                    int grown = (int) Math.min((long) counts.length * 2, limit);
                    keys = Arrays.copyOf(keys, grown);
                    counts = Arrays.copyOf(counts, grown);
                    if (sortValues != null) {
                        sortValues = Arrays.copyOf(sortValues, grown);
                    }
                    metrics.ensureCapacity(grown);
                    heap = Arrays.copyOf(heap, grown);
                }
                int idx = size;
                write(idx, numericKey, stringKey, count, sortValue);
                heap[size] = idx;
                siftUp(size++);
                return idx;
            }
            int worst = heap[0];
            if (compareCandidate(numericKey, stringKey, count, sortValue, worst) >= 0) {
                return -1;
            }
            write(worst, numericKey, stringKey, count, sortValue);
            metrics.reset(worst);
            siftDown(0);
            return worst;
        }

        private void write(int idx, long numericKey, BytesRef stringKey, long count, double sortValue) {
            if (keyKind == KeyKind.STRING) {
                if (dict == null) {
                    dict = new BytesRefHash();
                }
                int id = dict.add(stringKey);
                keys[idx] = id < 0 ? -id - 1 : id;
            } else {
                keys[idx] = numericKey;
            }
            counts[idx] = count;
            if (sortValues != null) {
                sortValues[idx] = sortValue;
            }
        }

        /** Negative when the candidate sorts before (better than) entry {@code idx}. */
        private int compareCandidate(long numericKey, BytesRef stringKey, long count, double sortValue, int idx) {
            int byValue = sortSlot < 0
                ? Long.compare(counts[idx], count)
                : Comparators.compareDiscardNaN(sortValue, sortValues[idx], sortAsc);
            if (byValue != 0) {
                return byValue;
            }
            if (keyKind == KeyKind.STRING) {
                dict.get((int) keys[idx], spare);
                return stringKey.compareTo(spare);
            }
            if (keyKind == KeyKind.DOUBLE) {
                return Double.compare(Double.longBitsToDouble(numericKey), Double.longBitsToDouble(keys[idx]));
            }
            return Long.compare(numericKey, keys[idx]);
        }

        /** Negative when entry {@code a} sorts before entry {@code b} in the selection order. */
        private int compare(int a, int b) {
            int byValue = sortSlot < 0
                ? Long.compare(counts[b], counts[a])
                : Comparators.compareDiscardNaN(sortValues[a], sortValues[b], sortAsc);
            if (byValue != 0) {
                return byValue;
            }
            if (keyKind == KeyKind.STRING) {
                dict.get((int) keys[a], spare);
                dict.get((int) keys[b], otherSpare);
                return spare.compareTo(otherSpare);
            }
            if (keyKind == KeyKind.DOUBLE) {
                return Double.compare(Double.longBitsToDouble(keys[a]), Double.longBitsToDouble(keys[b]));
            }
            return Long.compare(keys[a], keys[b]);
        }

        /** The heap's top is the weakest retained entry: a parent sorts after its children. */
        private void siftUp(int pos) {
            while (pos > 0) {
                int parent = (pos - 1) / 2;
                if (compare(heap[pos], heap[parent]) <= 0) {
                    return;
                }
                int swap = heap[pos];
                heap[pos] = heap[parent];
                heap[parent] = swap;
                pos = parent;
            }
        }

        private void siftDown(int pos) {
            while (true) {
                int worst = pos;
                int left = pos * 2 + 1;
                int right = left + 1;
                if (left < size && compare(heap[left], heap[worst]) > 0) {
                    worst = left;
                }
                if (right < size && compare(heap[right], heap[worst]) > 0) {
                    worst = right;
                }
                if (worst == pos) {
                    return;
                }
                int swap = heap[pos];
                heap[pos] = heap[worst];
                heap[worst] = swap;
                pos = worst;
            }
        }

        /** Folds the retained entries into {@code dest}, summing the counts and metrics of a key another scan also kept. */
        void mergeInto(GroupTable dest) {
            long[] encoded = new long[1];
            for (int e = 0; e < size; e++) {
                if (keyKind == KeyKind.STRING) {
                    dict.get((int) keys[e], spare);
                    encoded[0] = dest.intern(spare);
                } else {
                    encoded[0] = keys[e];
                }
                int idx = dest.findOrAdd(encoded, 0L);
                dest.addCount(idx, counts[e]);
                dest.metrics.merge(idx, metrics, e);
            }
        }
    }

    /**
     * The single level {@code terms} shape whose scans keep a bounded
     * top-k instead of every group: {@code sortSlot} is the metric the
     * order names (-1 for {@code _count} descending), {@code sortAsc}
     * its direction, and {@code perScanLimit} is {@code shard_size}
     * times {@code lance.aggregation.pushdown_topk_slack}, the groups
     * each scan retains. Only set when every metric of the plan is a
     * plain measure: a {@code cardinality} spreads a group over its
     * distinct values and a {@code percentiles} needs the bounds of
     * every group for its bin scan, so those shapes keep every group.
     */
    private record TopKSpec(KeyKind keyKind, int sortSlot, boolean sortAsc, int perScanLimit) {
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
     * One bucket level of the tree, outermost first. {@code column} and
     * {@code format} are null for a {@code filter} / {@code filters}
     * level, which has no field. For a {@code date_histogram},
     * {@code dateInterval} is the
     * {@code fixed_interval} in milliseconds the scan's key ordinal is
     * multiplied back by (0 for a calendar interval, whose key is
     * already the bucket start in millis) and {@code rounding} is the
     * rounding the aggregator would attach to its result; both are 0 /
     * null for the other kinds. For a mask level ({@link LevelKind#isMask}),
     * {@code branchKeys} names the buckets in order (the range keys,
     * null where the request left one unnamed; the filter keys; one
     * entry for {@code filter} / {@code missing}), {@code ranges} holds
     * the resolved ranges of a {@code range} / {@code date_range}, and
     * {@code otherBucketKey} is the {@code filters} other bucket's key,
     * null when the request did not ask for one.
     */
    private record Level(AggregationBuilder builder, LevelKind kind, Column column, KeyKind keyKind, DocValueFormat format, List<
        Child> children, List<Metric> metrics, long dateInterval, Rounding rounding, List<String> branchKeys,
        RangeAggregator.Range[] ranges, String otherBucketKey) {
        static Level keyed(
            ValuesSourceAggregationBuilder<?> builder,
            LevelKind kind,
            Column column,
            KeyKind keyKind,
            DocValueFormat format,
            List<Child> children,
            List<Metric> metrics,
            long dateInterval,
            Rounding rounding
        ) {
            return new Level(builder, kind, column, keyKind, format, children, metrics, dateInterval, rounding, null, null, null);
        }

        static Level mask(
            AggregationBuilder builder,
            LevelKind kind,
            Column column,
            DocValueFormat format,
            List<Child> children,
            List<Metric> metrics,
            List<String> branchKeys,
            RangeAggregator.Range[] ranges,
            String otherBucketKey
        ) {
            return new Level(builder, kind, column, KeyKind.LONG, format, children, metrics, 0L, null, branchKeys, ranges, otherBucketKey);
        }

        /** Number of buckets a mask level always reports, the other bucket included. */
        int bucketCount() {
            return branchKeys.size() + (otherBucketKey != null ? 1 : 0);
        }
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
     * {@code keyExpressions} are the bucket key groupings in key order,
     * kept so the percentiles bin scans can group by the same keys.
     * Public so the planner package's rule engine can carry it; the
     * constructor stays package private, so production plans are built
     * only inside this package.
     */
    public static final class Plan {
        private final ByteBuffer substrait;
        private final List<Expression> keyExpressions;
        private final List<Level> levels;
        private final Composite composite;
        private final List<Metric> topMetrics;
        private final List<Metric> allMetrics;
        private final int percentilesBins;
        private final TopKSpec topK;

        /**
         * Package private so the test sources' same package fixture
         * can build a minimal instance; production plans are still
         * built only by {@link #plan}.
         */
        Plan(
            ByteBuffer substrait,
            List<Expression> keyExpressions,
            List<Level> levels,
            Composite composite,
            List<Metric> topMetrics,
            List<Metric> allMetrics,
            int percentilesBins,
            TopKSpec topK
        ) {
            this.substrait = substrait;
            this.keyExpressions = keyExpressions;
            this.levels = levels;
            this.composite = composite;
            this.topMetrics = topMetrics;
            this.allMetrics = allMetrics;
            this.percentilesBins = percentilesBins;
            this.topK = topK;
        }

        private int keyCount() {
            return composite != null ? composite.sources().size() : levels.size();
        }

        /**
         * The encoded main scan, as a read only view. Package private
         * for the test sources' byte equivalence assertions between
         * two plans of the same request.
         */
        ByteBuffer substraitPlan() {
            return substrait.asReadOnlyBuffer();
        }

        private long dateInterval(int key) {
            return composite != null ? composite.sources().get(key).dateInterval() : levels.get(key).dateInterval();
        }

        private KeyKind keyKind(int key) {
            return composite != null ? composite.sources().get(key).keyKind() : levels.get(key).keyKind();
        }

        private KeyKind[] keyKinds() {
            KeyKind[] kinds = new KeyKind[keyCount()];
            for (int key = 0; key < kinds.length; key++) {
                kinds[key] = keyKind(key);
            }
            return kinds;
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
        Result execute(
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
            ScanPlan main = new ScanPlan(substrait, null, 0d, 0d, 0d);
            Partial merged = mergePartials(scans.runGroups(fragmentGroups, group -> scan(dataset, group, filterSql, main, cancellation)));
            int scanCount = fragmentGroups.size();
            for (Metric metric : allMetrics) {
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
            if (topK != null && partials.get(0).topK != null) {
                Partial merged = new Partial();
                GroupTable table = new GroupTable(keyKinds(), allMetrics);
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
            double width = max > min ? (max - min) / percentilesBins : 1d;
            SubstraitAggregatePlan.Builder builder = new SubstraitAggregatePlan.Builder();
            for (int key = 0; key < keyExpressions.size(); key++) {
                builder.groupBy(keyExpressions.get(key), KEY_COLUMN_PREFIX + key);
            }
            builder.groupBy(SubstraitExpressions.floorFp64(metric.column().numericExpression(), min, width), metric.binColumn());
            // count(field), not count(*): a row without a value has a null
            // bin and must not weigh in.
            builder.measure("count", List.of(new FieldReference(metric.column().index())), ScalarType.I64, metric.binCountColumn());
            return new ScanPlan(builder.build(), metric, min, max, width);
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
                if (keyCount() == 0) {
                    scanMetricsOnly(reader, plan, partial, cancellation);
                } else if (topK != null) {
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
                    MetricState[] states = new MetricState[allMetrics.size()];
                    for (int i = 0; i < states.length; i++) {
                        Metric metric = allMetrics.get(i);
                        if (plan.isMain()) {
                            states[i] = metric.read(root, row);
                        } else if (metric == plan.percentiles()) {
                            states[i] = metric.readBin(root, row, plan.min(), plan.max(), plan.width(), percentilesBins);
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
            TopKGroups groups = new TopKGroups(topK, allMetrics);
            partial.topK = groups;
            BytesRefBuilder scratch = new BytesRefBuilder();
            while (reader.loadNextBatch()) {
                cancellation.checkCancelled();
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                FieldVector counts = root.getVector(COUNT_COLUMN);
                FieldVector keyVector = root.getVector(KEY_COLUMN_PREFIX + 0);
                MetricBatch[] batches = MetricBatch.resolve(allMetrics, root);
                MetricBatch orderBatch = topK.sortSlot() >= 0 ? batches[topK.sortSlot()] : null;
                for (int row = 0; row < root.getRowCount(); row++) {
                    long count = longOrZero(counts, row);
                    partial.total += count;
                    if (keyVector.isNull(row)) {
                        // Documents without a value open no bucket.
                        continue;
                    }
                    long numericKey = 0L;
                    BytesRef stringKey = null;
                    switch (topK.keyKind()) {
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
            int keyCount = keyCount();
            GroupTable table = new GroupTable(keyKinds(), allMetrics);
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
                MetricBatch[] batches = plan.isMain() ? MetricBatch.resolve(allMetrics, root) : null;
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
                            opensBucket = key > 0 && composite == null;
                        } else {
                            encoded[key] = encodeKey(keyVectors[key], keyKind(key), row, dateInterval(key), table, scratch);
                            // A mask of 0 at the outermost level with no
                            // other bucket to hold it is the same: the
                            // row is in no bucket of the tree.
                            opensBucket = key > 0 || composite != null || inSomeBucket(levels.get(0), encoded[key]);
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
                                percentilesBins,
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
            if (keyCount() == 0) {
                // Lance returns exactly one row for a plan without
                // groupings, even over zero fragments; the fallback only
                // covers a reader that yielded no batch at all, and a
                // cardinality plan, whose distinct grouping returns no
                // row over no rows.
                GroupState state = merged.metricsOnly != null ? merged.metricsOnly : GroupState.empty(allMetrics.size());
                return new Result(toAggregations(topMetrics, state), merged.total, scans);
            }
            GroupTable table = merged.groups != null ? merged.groups : new GroupTable(keyKinds(), allMetrics);
            InternalAggregation aggregation;
            if (topK != null) {
                aggregation = assembleTopTerms(table, merged.keyedCount, dateHistogramPrototype);
            } else if (composite == null && levels.size() == 1 && isKeyOrderedTerms(levels.get(0))) {
                aggregation = assembleKeyedTerms(table, dateHistogramPrototype);
            } else {
                List<Group> groups = table.box();
                aggregation = composite != null ? buildComposite(groups) : buildLevel(0, groups, dateHistogramPrototype);
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
            Level level = levels.get(0);
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
            if (topK.sortSlot() < 0) {
                return (a, b) -> {
                    int byCount = Long.compare(table.countAt(b), table.countAt(a));
                    return byCount != 0 ? byCount : table.compareKeys(a, b);
                };
            }
            int slot = topK.sortSlot();
            boolean asc = topK.sortAsc();
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
            Level level = levels.get(0);
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
            Level level = levels.get(depth);
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
            Level level = levels.get(depth);
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
            Level level = levels.get(depth);
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
                            new InternalFilters.InternalBucket(
                                (String) bucket.key(),
                                bucket.count(),
                                subAggregations.get(i),
                                filters.isKeyed()
                            )
                        );
                    }
                    return new InternalFilters(filters.getName(), filterBuckets, filters.isKeyed(), metadata);
                }
                case FILTER -> {
                    return CoreAggregationResults.filter(
                        level.builder().getName(),
                        buckets.get(0).count(),
                        subAggregations.get(0),
                        metadata
                    );
                }
                case MISSING -> {
                    return CoreAggregationResults.missing(
                        level.builder().getName(),
                        buckets.get(0).count(),
                        subAggregations.get(0),
                        metadata
                    );
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
         * by key. A mask level reports every bucket with a count of 0 and
         * the empty sub aggregations.
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
     * the Lucene aggregators. Asks the
     * {@link AggregationRewriteRegistry} first: the first matching
     * rule's plan is the answer, and an empty answer falls through to
     * the shape dispatcher in the innermost overload.
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
        return planViaRegistry(
            aggregations,
            schema,
            multiFields,
            qsc,
            AggregationRewriteRegistry.instance(),
            maxGroups,
            percentilesBins,
            topkSlack
        );
    }

    /**
     * The registry consultation behind
     * {@link #plan(AggregatorFactories.Builder, Schema, Map, QueryShardContext)},
     * with the registry as a parameter so tests can drive the hook with
     * a curated rule set: the first matching rule's plan is the answer,
     * and an empty registry or an empty answer falls through to the
     * legacy shape dispatcher. The rules only see a tree that
     * {@link LanceAggregationSupport#isPushdownCandidate} accepts, the
     * same structural gate the legacy dispatcher applies, so moving a
     * shape from the dispatcher into a rule cannot widen what pushes
     * down. Skips building the context while the registry has no
     * rules, so an empty registry costs one list check.
     */
    static Plan planViaRegistry(
        AggregatorFactories.Builder aggregations,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        AggregationRewriteRegistry registry,
        int maxGroups,
        int bins,
        int slack
    ) {
        if (!registry.rules().isEmpty() && LanceAggregationSupport.isPushdownCandidate(aggregations)) {
            AggregationRewriteContext ctx = new AggregationRewriteContext(aggregations, schema, multiFields, qsc, maxGroups, bins, slack);
            Optional<PushdownPlan> rewritten = registry.rewrite(ctx);
            if (rewritten.isPresent()) {
                return rewritten.get().asLegacyPlan();
            }
        }
        return plan(aggregations, schema, multiFields, qsc, maxGroups, bins, slack);
    }

    /**
     * As {@link #plan(AggregatorFactories.Builder, Schema, Map, QueryShardContext)}
     * with an explicit bound on the estimated number of groups: the
     * product of the {@code shard_size} of every {@code terms} level and
     * the bucket count of every range / filters level. A tree whose
     * estimate exceeds {@code maxGroups} takes the aggregator path,
     * because the scan returns one row per key combination and this
     * node would hold them all. The distinct values a {@code cardinality}
     * groups by are not in the estimate: they are hashed into the sketch
     * as the rows are read and never held.
     */
    static Plan plan(
        AggregatorFactories.Builder aggregations,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        int maxGroups
    ) {
        return plan(aggregations, schema, multiFields, qsc, maxGroups, percentilesBins);
    }

    /** As above with an explicit percentiles bin count. */
    static Plan plan(
        AggregatorFactories.Builder aggregations,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        int maxGroups,
        int bins
    ) {
        return plan(aggregations, schema, multiFields, qsc, maxGroups, bins, topkSlack);
    }

    /** As above with an explicit top-k slack. */
    static Plan plan(
        AggregatorFactories.Builder aggregations,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        int maxGroups,
        int bins,
        int slack
    ) {
        if (!LanceAggregationSupport.isPushdownCandidate(aggregations)) {
            return null;
        }
        List<AggregationBuilder> top = new ArrayList<>(aggregations.getAggregatorFactories());
        if (LanceAggregationSupport.isPushdownMetric(top.get(0))) {
            return resolveMetricOnly(top, schema, multiFields, qsc, bins);
        }
        SubstraitAggregatePlan.Builder builder = new SubstraitAggregatePlan.Builder();
        List<Metric> allMetrics = new ArrayList<>();
        if (top.get(0) instanceof CompositeAggregationBuilder compositeBuilder) {
            List<Expression> keyExpressions = new ArrayList<>();
            Composite composite = resolveComposite(compositeBuilder, schema, multiFields, qsc, keyExpressions, allMetrics);
            if (composite == null) {
                return null;
            }
            return new Plan(
                finish(builder, keyExpressions, allMetrics),
                keyExpressions,
                List.of(),
                composite,
                List.of(),
                allMetrics,
                bins,
                null
            );
        }
        List<Level> levels = new ArrayList<>();
        List<Expression> keyExpressions = new ArrayList<>();
        long estimatedGroups = 1L;
        AggregationBuilder current = top.get(0);
        while (current != null) {
            Level level;
            Expression keyExpression;
            if (current instanceof FilterAggregationBuilder || current instanceof FiltersAggregationBuilder) {
                List<QueryBuilder> queries = new ArrayList<>();
                List<String> keys = new ArrayList<>();
                String otherBucketKey = null;
                LevelKind kind;
                if (current instanceof FilterAggregationBuilder filter) {
                    kind = LevelKind.FILTER;
                    queries.add(filter.getFilter());
                    keys.add(filter.getName());
                } else {
                    FiltersAggregationBuilder filters = (FiltersAggregationBuilder) current;
                    kind = LevelKind.FILTERS;
                    for (FiltersAggregator.KeyedFilter keyed : filters.filters()) {
                        queries.add(keyed.filter());
                        keys.add(keyed.key());
                    }
                    otherBucketKey = filters.otherBucket() ? filters.otherBucketKey() : null;
                }
                List<Expression> conditions = new ArrayList<>(queries.size());
                for (QueryBuilder query : queries) {
                    Expression condition = FilterPredicates.predicate(query, schema, multiFields, qsc);
                    if (condition == null) {
                        return null;
                    }
                    conditions.add(condition);
                }
                keyExpression = SubstraitExpressions.matchMask(conditions);
                estimatedGroups = saturatingMultiply(estimatedGroups, keys.size() + 1L);
                level = Level.mask(current, kind, null, null, null, null, keys, null, otherBucketKey);
            } else {
                ValuesSourceAggregationBuilder<?> bucketBuilder = (ValuesSourceAggregationBuilder<?>) current;
                Column column = resolveColumn(bucketBuilder.field(), schema, multiFields, qsc);
                if (column == null) {
                    return null;
                }
                DocValueFormat format = column.fieldType().docValueFormat(bucketBuilder.format(), bucketBuilder.timeZone());
                KeyKind keyKind = KeyKind.LONG;
                long dateInterval = 0L;
                Rounding rounding = null;
                LevelKind kind;
                List<String> branchKeys = null;
                RangeAggregator.Range[] ranges = null;
                if (bucketBuilder instanceof TermsAggregationBuilder terms) {
                    kind = LevelKind.TERMS;
                    if (column.isUtf8()) {
                        keyExpression = new FieldReference(column.index());
                        keyKind = KeyKind.STRING;
                    } else if (column.isFloating()) {
                        keyExpression = new FieldReference(column.index());
                        keyKind = KeyKind.DOUBLE;
                    } else {
                        keyExpression = column.numericExpression();
                    }
                    // Every terms level multiplies the combinations the scan
                    // may return by the groups this level keeps.
                    estimatedGroups = saturatingMultiply(estimatedGroups, thresholds(terms).getShardSize());
                } else if (bucketBuilder instanceof HistogramAggregationBuilder histogram) {
                    kind = LevelKind.HISTOGRAM;
                    if (column.isUtf8() || column.isDate() || column.isBoolean()) {
                        return null;
                    }
                    keyExpression = SubstraitExpressions.floorFp64(
                        new FieldReference(column.index()),
                        histogram.offset(),
                        histogram.interval()
                    );
                } else if (bucketBuilder instanceof DateHistogramAggregationBuilder dateHistogram) {
                    kind = LevelKind.DATE_HISTOGRAM;
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
                        keyExpression = SubstraitExpressions.floorDivInt64(
                            column.numericExpression(),
                            dateHistogram.offset(),
                            dateInterval
                        );
                        rounding = Rounding.builder(TimeValue.timeValueMillis(dateInterval)).build();
                    }
                } else if (bucketBuilder instanceof AbstractRangeBuilder<?, ?> rangeBuilder) {
                    // range takes any number; date_range the date columns
                    // only, whose bounds the date format parses. Booleans
                    // and keywords take the aggregator path.
                    boolean date = rangeBuilder instanceof DateRangeAggregationBuilder;
                    if (column.isUtf8() || column.isBoolean() || (date && !column.isDate())) {
                        return null;
                    }
                    kind = date ? LevelKind.DATE_RANGE : LevelKind.RANGE;
                    ranges = resolveRanges(rangeBuilder, format, date, qsc);
                    if (ranges == null) {
                        return null;
                    }
                    branchKeys = new ArrayList<>(ranges.length);
                    List<Expression> conditions = new ArrayList<>(ranges.length);
                    Expression value = new Cast(column.numericExpression(), ScalarType.FP64);
                    for (RangeAggregator.Range range : ranges) {
                        branchKeys.add(range.getKey());
                        conditions.add(rangeCondition(value, range.getFrom(), range.getTo()));
                    }
                    keyExpression = SubstraitExpressions.matchMask(conditions);
                    estimatedGroups = saturatingMultiply(estimatedGroups, ranges.length + 1L);
                } else {
                    kind = LevelKind.MISSING;
                    branchKeys = List.of(bucketBuilder.getName());
                    keyExpression = SubstraitExpressions.matchMask(
                        List.of(SubstraitExpressions.isNull(new FieldReference(column.index())))
                    );
                }
                if (kind.isMask()) {
                    level = Level.mask(bucketBuilder, kind, column, format, null, null, branchKeys, ranges, null);
                } else {
                    level = Level.keyed(bucketBuilder, kind, column, keyKind, format, null, null, dateInterval, rounding);
                }
            }
            if (estimatedGroups > maxGroups) {
                return null;
            }
            List<Child> children = new ArrayList<>();
            List<Metric> metrics = new ArrayList<>();
            AggregationBuilder nested = null;
            for (AggregationBuilder sub : current.getSubAggregations()) {
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
            keyExpressions.add(keyExpression);
            levels.add(
                new Level(
                    level.builder(),
                    level.kind(),
                    level.column(),
                    level.keyKind(),
                    level.format(),
                    children,
                    metrics,
                    level.dateInterval(),
                    level.rounding(),
                    level.branchKeys(),
                    level.ranges(),
                    level.otherBucketKey()
                )
            );
            current = nested;
        }
        TopKSpec topK = resolveTopK(levels, allMetrics, slack);
        // A terms order that is neither a count nor a key order is
        // honoured by the top-k selection of the single level shape
        // only; a tree it did not resolve for takes the aggregators.
        for (Level level : levels) {
            if (level.builder() instanceof TermsAggregationBuilder termsBuilder
                && !InternalOrder.isCountDesc(termsBuilder.order())
                && !InternalOrder.isKeyOrder(termsBuilder.order())
                && (topK == null || topK.sortSlot() < 0)) {
                return null;
            }
        }
        return new Plan(finish(builder, keyExpressions, allMetrics), keyExpressions, levels, null, List.of(), allMetrics, bins, topK);
    }

    /**
     * The plan of a metric only tree: every top level aggregation is a
     * metric the scan computes and there is no bucket level, so the
     * scan groups by nothing and the plan carries no key expressions,
     * no composite and no top-k. Null when the first top level
     * aggregation is not such a metric (the tree is not this shape) or
     * when any metric fails to resolve against the schema and the
     * mapping, in which case the aggregators answer. Public because
     * the planner package's metric only rule is the production caller;
     * the shape dispatcher above delegates here too, so the rule path
     * and the fall through path are one code path.
     *
     * @param top  the request's top level aggregation builders, in
     *             request order
     * @param bins bins of a pushed down percentiles histogram
     */
    public static Plan resolveMetricOnly(
        List<AggregationBuilder> top,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        int bins
    ) {
        if (top.isEmpty() || !LanceAggregationSupport.isPushdownMetric(top.get(0))) {
            return null;
        }
        SubstraitAggregatePlan.Builder builder = new SubstraitAggregatePlan.Builder();
        List<Metric> allMetrics = new ArrayList<>();
        List<Metric> metrics = resolveMetrics(top, schema, multiFields, qsc, allMetrics);
        if (metrics == null) {
            return null;
        }
        return new Plan(finish(builder, List.of(), allMetrics), List.of(), List.of(), null, metrics, allMetrics, bins, null);
    }

    /**
     * The top-k selection of a single {@code terms} level ordered by
     * {@code _count} descending or by one of its own single value
     * metric children ({@code sum}, {@code avg}, {@code min},
     * {@code max}, {@code value_count}, named as {@code m} or
     * {@code m.value}); null for every other tree, which keeps every
     * group. Plans with a {@code cardinality} or {@code percentiles}
     * anywhere keep every group too: the first spreads a group over
     * its distinct values, the second needs every group's bounds for
     * its bin scan.
     */
    private static TopKSpec resolveTopK(List<Level> levels, List<Metric> allMetrics, int slack) {
        if (levels.size() != 1 || levels.get(0).kind() != LevelKind.TERMS) {
            return null;
        }
        for (Metric metric : allMetrics) {
            if (metric.kind() == MetricKind.CARDINALITY || metric.isPercentiles()) {
                return null;
            }
        }
        Level level = levels.get(0);
        TermsAggregationBuilder terms = (TermsAggregationBuilder) level.builder();
        BucketOrder order = terms.order();
        int shardSize = thresholds(terms).getShardSize();
        int limit = (int) Math.min((long) shardSize * slack, Integer.MAX_VALUE - 8);
        if (InternalOrder.isCountDesc(order)) {
            return new TopKSpec(level.keyKind(), -1, false, limit);
        }
        LanceAggregationSupport.AggregationOrder aggregationOrder = LanceAggregationSupport.aggregationOrder(order);
        if (aggregationOrder == null) {
            return null;
        }
        for (Metric metric : level.metrics()) {
            boolean singleValue = switch (metric.kind()) {
                case SUM, AVG, MIN, MAX, VALUE_COUNT -> true;
                default -> false;
            };
            if (singleValue
                && (metric.name().equals(aggregationOrder.path()) || (metric.name() + ".value").equals(aggregationOrder.path()))) {
                return new TopKSpec(level.keyKind(), metric.slot(), aggregationOrder.ascending(), limit);
            }
        }
        return null;
    }

    /**
     * Encodes the first scan: the bucket key groupings in order, the
     * distinct value grouping of a {@code cardinality} after them,
     * {@code count(*)} and every metric's measures. At most one
     * {@code cardinality} per plan: a second would multiply the rows by
     * the distinct values of both fields ({@link #resolveMetric} refuses
     * it).
     */
    private static ByteBuffer finish(SubstraitAggregatePlan.Builder builder, List<Expression> keyExpressions, List<Metric> allMetrics) {
        for (int key = 0; key < keyExpressions.size(); key++) {
            builder.groupBy(keyExpressions.get(key), KEY_COLUMN_PREFIX + key);
        }
        for (Metric metric : allMetrics) {
            if (metric.kind() == MetricKind.CARDINALITY) {
                builder.groupBy(metric.distinctExpression(), metric.distinctColumn());
            }
        }
        builder.measure("count", List.of(), ScalarType.I64, COUNT_COLUMN);
        for (Metric metric : allMetrics) {
            metric.addMeasures(builder);
        }
        return builder.build();
    }

    /**
     * The request's ranges with their bounds resolved and sorted the way
     * the range aggregator factory prepares them: a string bound is
     * parsed by the field's format ({@code now} included), a numeric
     * bound of a {@code date_range} is parsed as text as well so numeric
     * formats such as {@code epoch_second} apply, and the ranges are
     * ordered by {@code from} then {@code to}. Null when a bound does
     * not parse, in which case the aggregator raises the request error.
     */
    private static RangeAggregator.Range[] resolveRanges(
        AbstractRangeBuilder<?, ?> builder,
        DocValueFormat format,
        boolean date,
        QueryShardContext qsc
    ) {
        List<? extends RangeAggregator.Range> requested = builder.ranges();
        RangeAggregator.Range[] ranges = new RangeAggregator.Range[requested.size()];
        try {
            for (int i = 0; i < ranges.length; i++) {
                RangeAggregator.Range range = requested.get(i);
                double from = range.getFrom();
                double to = range.getTo();
                if (range.getFromAsString() != null) {
                    from = format.parseDouble(range.getFromAsString(), false, qsc::nowInMillis);
                } else if (date && Double.isFinite(from)) {
                    from = format.parseDouble(Long.toString((long) from), false, qsc::nowInMillis);
                }
                if (range.getToAsString() != null) {
                    to = format.parseDouble(range.getToAsString(), false, qsc::nowInMillis);
                } else if (date && Double.isFinite(to)) {
                    to = format.parseDouble(Long.toString((long) to), false, qsc::nowInMillis);
                }
                ranges[i] = new RangeAggregator.Range(range.getKey(), from, range.getFromAsString(), to, range.getToAsString());
            }
        } catch (RuntimeException unparseable) {
            return null;
        }
        Arrays.sort(ranges, Comparator.comparingDouble(RangeAggregator.Range::getFrom).thenComparingDouble(RangeAggregator.Range::getTo));
        return ranges;
    }

    /** {@code value >= from AND value < to} on doubles, an infinite bound left out. */
    private static Expression rangeCondition(Expression value, double from, double to) {
        Expression lower = Double.isInfinite(from) ? null : ScalarFunction.of("gte", value, new Float64Literal(from));
        Expression upper = Double.isInfinite(to) ? null : ScalarFunction.of("lt", value, new Float64Literal(to));
        if (lower == null && upper == null) {
            return SubstraitExpressions.isNotNull(value);
        }
        if (lower == null) {
            return upper;
        }
        return upper == null ? lower : SubstraitExpressions.and(lower, upper);
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
        List<Expression> keyExpressions,
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
            keyExpressions.add(keyExpression);
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
        } else if (builder instanceof StatsAggregationBuilder) {
            kind = MetricKind.STATS;
        } else if (builder instanceof ExtendedStatsAggregationBuilder) {
            kind = MetricKind.EXTENDED_STATS;
        } else if (builder instanceof CardinalityAggregationBuilder) {
            kind = MetricKind.CARDINALITY;
            // A second distinct grouping would multiply the rows by the
            // distinct values of both fields.
            for (Metric other : allMetrics) {
                if (other.kind() == MetricKind.CARDINALITY) {
                    return null;
                }
            }
        } else if (builder instanceof PercentilesAggregationBuilder) {
            kind = MetricKind.PERCENTILES;
        } else if (builder instanceof PercentileRanksAggregationBuilder) {
            kind = MetricKind.PERCENTILE_RANKS;
        } else {
            return null;
        }
        // Arithmetic metrics need a number; value_count and cardinality
        // only need the column to be single valued, which every accepted
        // type is.
        if (kind != MetricKind.VALUE_COUNT && kind != MetricKind.CARDINALITY && column.isUtf8()) {
            return null;
        }
        DocValueFormat format = column.fieldType().docValueFormat(source.format(), source.timeZone());
        Metric metric = new Metric(builder.getName(), kind, column, format, metadata(builder), allMetrics.size(), source);
        allMetrics.add(metric);
        return metric;
    }

    /**
     * The {@code precision_threshold} of a cardinality builder, null
     * when the request named none. The builder has a setter but no
     * getter, so it is read back from the builder's own JSON rendering,
     * which writes the key only when the option was set.
     */
    @SuppressWarnings("unchecked")
    private static Long precisionThreshold(CardinalityAggregationBuilder cardinality) {
        String json = Strings.toString(XContentType.JSON, cardinality);
        Map<String, Object> rendered = XContentHelper.convertToMap(new BytesArray(json), false, XContentType.JSON).v2();
        Map<String, Object> body = (Map<String, Object>) rendered.get(cardinality.getName());
        Map<String, Object> definition = (Map<String, Object>) body.get(CardinalityAggregationBuilder.NAME);
        Object threshold = definition.get(CardinalityAggregationBuilder.PRECISION_THRESHOLD_FIELD.getPreferredName());
        return threshold instanceof Number number ? number.longValue() : null;
    }

    /**
     * Turns the query of a {@code filter} / {@code filters} bucket into
     * a Substrait predicate over the table columns, with the semantics
     * the Lucene query the aggregator would run has: {@code term} and
     * {@code terms} compare in the field's own type (a float literal in
     * single precision, a date through the field's date format with the
     * day rounding the date field type applies), {@code range} bounds
     * likewise, {@code exists} is {@code IS NOT NULL}, and a {@code bool}
     * ANDs its {@code must} / {@code filter} clauses, negates
     * {@code must_not} without excluding rows that have no value, and
     * requires one {@code should} only when there is no {@code must} /
     * {@code filter}, as {@code BooleanQuery} does with no
     * {@code minimum_should_match}. Every other query, a value the field
     * type would reject, an unmapped field or a column the pushdown does
     * not handle yields null and the request takes the aggregator path.
     */
    private static final class FilterPredicates {

        private FilterPredicates() {}

        static Expression predicate(
            QueryBuilder query,
            Schema schema,
            Map<String, LinkedHashMap<String, String>> multiFields,
            QueryShardContext qsc
        ) {
            if (query == null || query instanceof MatchAllQueryBuilder) {
                return new BoolLiteral(true);
            }
            if (query instanceof TermQueryBuilder term) {
                Column column = resolveColumn(term.fieldName(), schema, multiFields, qsc);
                return column == null ? null : equalTo(column, term.value(), qsc);
            }
            if (query instanceof TermsQueryBuilder terms) {
                Column column = resolveColumn(terms.fieldName(), schema, multiFields, qsc);
                if (column == null) {
                    return null;
                }
                if (terms.values() == null || terms.values().isEmpty()) {
                    return new BoolLiteral(false);
                }
                Expression any = null;
                for (Object value : terms.values()) {
                    Expression equal = equalTo(column, value, qsc);
                    if (equal == null) {
                        return null;
                    }
                    any = any == null ? equal : SubstraitExpressions.or(any, equal);
                }
                return any;
            }
            if (query instanceof ExistsQueryBuilder exists) {
                Column column = resolveColumn(exists.fieldName(), schema, multiFields, qsc);
                return column == null ? null : SubstraitExpressions.isNotNull(new FieldReference(column.index()));
            }
            if (query instanceof RangeQueryBuilder range) {
                Column column = resolveColumn(range.fieldName(), schema, multiFields, qsc);
                return column == null ? null : rangeOf(column, range, qsc);
            }
            if (query instanceof BoolQueryBuilder bool) {
                return boolOf(bool, schema, multiFields, qsc);
            }
            return null;
        }

        private static Expression boolOf(
            BoolQueryBuilder bool,
            Schema schema,
            Map<String, LinkedHashMap<String, String>> multiFields,
            QueryShardContext qsc
        ) {
            if (bool.minimumShouldMatch() != null) {
                return null;
            }
            Expression all = null;
            for (QueryBuilder clause : bool.must()) {
                all = conjoin(all, predicate(clause, schema, multiFields, qsc));
                if (all == null) {
                    return null;
                }
            }
            for (QueryBuilder clause : bool.filter()) {
                all = conjoin(all, predicate(clause, schema, multiFields, qsc));
                if (all == null) {
                    return null;
                }
            }
            boolean required = bool.must().isEmpty() && bool.filter().isEmpty();
            if (required && !bool.should().isEmpty()) {
                Expression any = null;
                for (QueryBuilder clause : bool.should()) {
                    Expression one = predicate(clause, schema, multiFields, qsc);
                    if (one == null) {
                        return null;
                    }
                    any = any == null ? one : SubstraitExpressions.or(any, one);
                }
                all = any;
            }
            if (all == null && !bool.mustNot().isEmpty() && !bool.adjustPureNegative()) {
                // A purely negative BooleanQuery matches nothing unless
                // the builder adds the match_all it does by default.
                return null;
            }
            for (QueryBuilder clause : bool.mustNot()) {
                Expression excluded = predicate(clause, schema, multiFields, qsc);
                if (excluded == null) {
                    return null;
                }
                all = conjoin(all, SubstraitExpressions.notTrue(excluded));
            }
            return all == null ? new BoolLiteral(true) : all;
        }

        private static Expression conjoin(Expression left, Expression right) {
            if (right == null) {
                return null;
            }
            return left == null ? right : SubstraitExpressions.and(left, right);
        }

        /** {@code column = value} in the column's type, or null for a value the field type would not accept. */
        private static Expression equalTo(Column column, Object value, QueryShardContext qsc) {
            if (value == null) {
                return null;
            }
            if (column.isUtf8()) {
                return ScalarFunction.of("equal", new FieldReference(column.index()), new StringLiteral(text(value)));
            }
            if (column.isDate()) {
                // The date field type answers a term with the range
                // [floor, ceiling] of the value's precision.
                Long lower = parseDate(column, null, value, false, qsc);
                Long upper = parseDate(column, null, value, true, qsc);
                if (lower == null || upper == null) {
                    return null;
                }
                Expression millis = column.numericExpression();
                return SubstraitExpressions.and(
                    ScalarFunction.of("gte", millis, new Int64Literal(lower)),
                    ScalarFunction.of("lte", millis, new Int64Literal(upper))
                );
            }
            if (column.isBoolean()) {
                Boolean flag = bool(value);
                return flag == null
                    ? null
                    : ScalarFunction.of(
                        "equal",
                        new Cast(new FieldReference(column.index()), ScalarType.I64),
                        new Int64Literal(flag ? 1L : 0L)
                    );
            }
            if (column.isFloating()) {
                Double number = floating(column, value);
                return number == null
                    ? null
                    : ScalarFunction.of("equal", new Cast(new FieldReference(column.index()), ScalarType.FP64), new Float64Literal(number));
            }
            Long number = integral(value);
            return number == null
                ? null
                : ScalarFunction.of("equal", new Cast(new FieldReference(column.index()), ScalarType.I64), new Int64Literal(number));
        }

        /**
         * The range query's bounds as the field type resolves them: a
         * date bound parsed by the field's format with the query's own
         * {@code format} / {@code time_zone}, rounded up for an exclusive
         * lower or inclusive upper bound and then moved off the excluded
         * millisecond; a number in the column's precision. Keyword and
         * boolean ranges take the aggregator path.
         */
        private static Expression rangeOf(Column column, RangeQueryBuilder range, QueryShardContext qsc) {
            if (column.isUtf8() || column.isBoolean() || (range.from() == null && range.to() == null)) {
                return null;
            }
            if (column.isDate()) {
                Expression millis = column.numericExpression();
                Expression lower = null;
                Expression upper = null;
                if (range.from() != null) {
                    Long from = parseDate(column, range, range.from(), !range.includeLower(), qsc);
                    if (from == null) {
                        return null;
                    }
                    lower = ScalarFunction.of("gte", millis, new Int64Literal(range.includeLower() ? from : from + 1L));
                }
                if (range.to() != null) {
                    Long to = parseDate(column, range, range.to(), range.includeUpper(), qsc);
                    if (to == null) {
                        return null;
                    }
                    upper = ScalarFunction.of("lte", millis, new Int64Literal(range.includeUpper() ? to : to - 1L));
                }
                return lower == null ? upper : upper == null ? lower : SubstraitExpressions.and(lower, upper);
            }
            Expression value;
            Expression lowerBound = null;
            Expression upperBound = null;
            if (column.isFloating()) {
                value = new Cast(new FieldReference(column.index()), ScalarType.FP64);
                if (range.from() != null) {
                    Double from = floating(column, range.from());
                    if (from == null) {
                        return null;
                    }
                    lowerBound = new Float64Literal(from);
                }
                if (range.to() != null) {
                    Double to = floating(column, range.to());
                    if (to == null) {
                        return null;
                    }
                    upperBound = new Float64Literal(to);
                }
            } else {
                value = new Cast(new FieldReference(column.index()), ScalarType.I64);
                if (range.from() != null) {
                    Long from = integral(range.from());
                    if (from == null) {
                        return null;
                    }
                    lowerBound = new Int64Literal(from);
                }
                if (range.to() != null) {
                    Long to = integral(range.to());
                    if (to == null) {
                        return null;
                    }
                    upperBound = new Int64Literal(to);
                }
            }
            Expression lower = lowerBound == null ? null : ScalarFunction.of(range.includeLower() ? "gte" : "gt", value, lowerBound);
            Expression upper = upperBound == null ? null : ScalarFunction.of(range.includeUpper() ? "lte" : "lt", value, upperBound);
            return lower == null ? upper : upper == null ? lower : SubstraitExpressions.and(lower, upper);
        }

        /**
         * Epoch millis of a date bound through the field's date format
         * (the range query's {@code format} and {@code time_zone} when it
         * names them), null when the text does not parse.
         */
        private static Long parseDate(Column column, RangeQueryBuilder range, Object value, boolean roundUp, QueryShardContext qsc) {
            try {
                String pattern = range == null ? null : range.format();
                ZoneId zone = range == null || range.timeZone() == null ? null : ZoneId.of(range.timeZone());
                DocValueFormat format = column.fieldType().docValueFormat(pattern, zone);
                return format.parseLong(text(value), roundUp, qsc::nowInMillis);
            } catch (RuntimeException unparseable) {
                return null;
            }
        }

        private static String text(Object value) {
            return value instanceof BytesRef bytes ? bytes.utf8ToString() : String.valueOf(value);
        }

        private static Boolean bool(Object value) {
            if (value instanceof Boolean flag) {
                return flag;
            }
            String text = text(value);
            if (text.equals("true")) {
                return true;
            }
            return text.equals("false") ? false : null;
        }

        /** A whole number, or null: a fractional value on an integer column has rounding rules the pushdown does not replicate. */
        private static Long integral(Object value) {
            if (value instanceof Boolean) {
                return null;
            }
            double number;
            if (value instanceof Number n) {
                number = n.doubleValue();
            } else {
                try {
                    number = Double.parseDouble(text(value));
                } catch (NumberFormatException unparseable) {
                    return null;
                }
            }
            if (!Double.isFinite(number) || number != Math.rint(number) || Math.abs(number) > 9.007199254740992E15d) {
                return null;
            }
            return (long) number;
        }

        /** The value in the column's precision: a float column compares in single precision, as the float field type does. */
        private static Double floating(Column column, Object value) {
            if (value instanceof Boolean) {
                return null;
            }
            double number;
            if (value instanceof Number n) {
                number = n.doubleValue();
            } else {
                try {
                    number = Double.parseDouble(text(value));
                } catch (NumberFormatException unparseable) {
                    return null;
                }
            }
            if (Double.isNaN(number)) {
                return null;
            }
            boolean single = column.type() instanceof ArrowType.FloatingPoint fp && fp.getPrecision() == FloatingPointPrecision.SINGLE;
            return single ? (double) (float) number : number;
        }
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

    private static double doubleKeyOf(FieldVector vector, int row) {
        if (vector instanceof Float4Vector v) {
            return v.get(row);
        }
        if (vector instanceof Float8Vector v) {
            return v.get(row);
        }
        throw new IllegalStateException(
            "unexpected Arrow vector " + vector.getClass().getSimpleName() + " for a floating point key " + vector.getName()
        );
    }

    /**
     * The row's UTF-8 bytes read into {@code scratch} through the
     * vector's offset and data buffers, so the row loop copies bytes
     * instead of allocating an array per row as {@code VarCharVector#get}
     * does.
     */
    private static BytesRef readUtf8(VarCharVector vector, int row, BytesRefBuilder scratch) {
        long start = vector.getOffsetBuffer().getInt(row * 4L);
        int length = vector.getOffsetBuffer().getInt((row + 1) * 4L) - (int) start;
        scratch.grow(length);
        vector.getDataBuffer().getBytes(start, scratch.bytes(), 0, length);
        scratch.setLength(length);
        return scratch.get();
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
