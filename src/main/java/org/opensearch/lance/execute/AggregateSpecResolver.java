/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.execute;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Rounding;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BitMixer;
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
import org.opensearch.lance.dispatch.LanceAggregationSupport;
import org.opensearch.lance.execute.GroupAggregationState.GroupState;
import org.opensearch.lance.execute.GroupAggregationState.MetricState;
import org.opensearch.lance.execute.LanceAggregateResults.PushedShape;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.BucketUtils;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.AbstractRangeBuilder;
import org.opensearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.RangeAggregator;
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

import static org.opensearch.lance.execute.ArrowRowValues.asLong;
import static org.opensearch.lance.execute.ArrowRowValues.doubleOr;
import static org.opensearch.lance.execute.ArrowRowValues.doubleOrZero;
import static org.opensearch.lance.execute.ArrowRowValues.longOrZero;

/**
 * Pairs the request's aggregation builders with the pushed aggregate
 * and prepares the response side state the planner's specs do not
 * model: which Lance column each field maps to and how its key is
 * encoded ({@link Column}, {@link KeyKind}), the metric kinds and
 * their result column names ({@link Metric}, {@link MetricKind}), the
 * bucket levels of a nested tree with their Lucene doc value formats,
 * roundings, resolved ranges and filter keys ({@link Level},
 * {@link LevelKind}, {@link Child}), the composite sources with their
 * parsed {@code after} values ({@link Source}, {@link Composite}), and
 * the bounded top-k selection of the single level {@code terms} shape
 * ({@link TopKSpec}). {@link #resolve} returns a
 * {@link ResolvedAggregate} the scan runner and the result assembler
 * consume, or null when the executor cannot own the plan and the
 * request stays on the Lucene aggregators. {@link FilterChecks}
 * mirrors the mask predicates of a {@code filter} / {@code filters}
 * bucket against the mapping. Nothing here opens a scan or builds a
 * result; the spec records carry the per row readers
 * ({@link Metric#read}) and result constructors
 * ({@link Metric#toAggregation}) the other classes call.
 */
final class AggregateSpecResolver {

    private AggregateSpecResolver() {}

    /** {@code after} values are literals; the aggregators reject {@code now} in them the same way. */
    private static final LongSupplier NO_NOW_IN_AFTER = () -> {
        throw new IllegalArgumentException("now() is not supported in [after] key");
    };

    enum MetricKind {
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

    enum KeyKind {
        STRING,
        LONG,
        DOUBLE
    }

    /**
     * How a bucket level turns its key into buckets. The first three
     * open one bucket per distinct key; the mask kinds read the key as
     * the bit set of the ranges or filters the row falls in
     * (the CASE WHEN bit mask the translator projects) and put the row into
     * every bucket whose bit is set.
     */
    enum LevelKind {
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
    record Column(String name, int index, ArrowType type, MappedFieldType fieldType) {
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
    record Metric(String name, MetricKind kind, Column column, DocValueFormat format, Map<String, Object> metadata, int slot,
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

        /** The grouping expression a {@code cardinality} adds: the value as the aggregator hashes it. */
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
    record TopKSpec(KeyKind keyKind, int sortSlot, boolean sortAsc, int perScanLimit) {
    }

    /**
     * One sub aggregation slot of a bucket level, in request order so
     * the bucket's aggregations come out in the order the aggregators
     * would emit them: a metric, or ({@code metric} null) the nested
     * bucket level.
     */
    record Child(Metric metric) {
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
    record Level(AggregationBuilder builder, LevelKind kind, Column column, KeyKind keyKind, DocValueFormat format, List<Child> children,
        List<Metric> metrics, long dateInterval, Rounding rounding, List<String> branchKeys, RangeAggregator.Range[] ranges,
        String otherBucketKey) {
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
    record Source(CompositeValuesSourceBuilder<?> builder, Column column, KeyKind keyKind, DocValueFormat format, int reverseMul,
        Comparable<?> after, long dateInterval) {
    }

    record Composite(CompositeAggregationBuilder builder, List<Source> sources, List<Metric> metrics) {
    }

    /**
     * Prepares the executor state of one pushed aggregate: walks the
     * request's builders the way the translator walked them, resolves
     * every field against the mapping (formats, key kinds, rounding),
     * derives the terms top-k selection and bounds the group estimate
     * by {@code maxGroups}. Returns null when the executor cannot own
     * the plan (an unmapped field, a group estimate over the bound, a
     * terms order the top-k selection cannot honour), in which case the
     * request stays on the Lucene aggregators.
     *
     * <p>The group bound here is a node local guard, not a routing
     * decision. The coordinator's planner already compared its own
     * estimate, built from the table statistics (the distinct counts of
     * the bitmap indexes, the date intervals, the range and filter
     * counts) when every key has one, with the same
     * {@code lance.aggregation.pushdown_max_groups} and priced the
     * pushed form as infinite when it exceeded the bound; this estimate
     * is built from the request shape instead ({@code shard_size} of
     * every terms level, the range and filter counts) and protects the
     * executor's group state when the two disagree or the statistics
     * had nothing to say. A refusal here is
     * counted as {@code plan.refinements.aggregate_resolution} and the
     * request runs on the Lucene aggregators.
     *
     * @param shape the pushed aggregate's group key count and metric
     *     slots, as the plan the coordinator shipped carries them
     * @param substrait the encoded main scan, a direct buffer
     * @param aggregations the request's aggregation builders
     * @param schema the dataset's Arrow schema
     * @param multiFields the index's keyword sub-field spec
     * @param qsc the mapping of the index the request targets
     * @param maxGroups the bound on the estimated number of groups,
     *     {@code lance.aggregation.pushdown_max_groups} read from the
     *     node settings
     */
    static ResolvedAggregate resolve(
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
        List<AggregationBuilder> top = new ArrayList<>(aggregations.getAggregatorFactories());
        if (top.isEmpty()) {
            return null;
        }
        ResolvedAggregate resolved;
        if (LanceAggregationSupport.isPushdownMetric(top.get(0))) {
            List<Metric> allMetrics = new ArrayList<>();
            List<Metric> metrics = resolveMetrics(top, schema, multiFields, qsc, allMetrics);
            resolved = metrics == null ? null : new ResolvedAggregate(shape, substrait, List.of(), null, metrics, allMetrics, bins, null);
        } else if (top.size() == 1 && top.get(0) instanceof CompositeAggregationBuilder composite) {
            resolved = resolveCompositeShape(shape, substrait, composite, schema, multiFields, qsc, bins);
        } else if (top.size() == 1) {
            resolved = resolveBucketTree(shape, substrait, top.get(0), schema, multiFields, qsc, maxGroups, bins, slack);
        } else {
            return null;
        }
        return resolved != null && resolved.matchesSpecs() ? resolved : null;
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
    static Map<String, Object> metadata(AggregationBuilder builder) {
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
    static TermsAggregator.BucketCountThresholds thresholds(TermsAggregationBuilder terms) {
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

    /**
     * The executor state of a nested bucket tree, one level per nesting
     * step, mirroring the translator's walk. Null when a level or a
     * metric fails to resolve against the schema and the mapping, when
     * the group estimate exceeds {@code maxGroups}, or when a terms
     * order needs a top-k selection this tree does not support.
     */
    private static ResolvedAggregate resolveBucketTree(
        PushedShape shape,
        ByteBuffer substrait,
        AggregationBuilder root,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        int maxGroups,
        int bins,
        int slack
    ) {
        List<Metric> allMetrics = new ArrayList<>();
        List<Level> levels = new ArrayList<>();
        long estimatedGroups = 1L;
        AggregationBuilder current = root;
        while (current != null) {
            LevelKind kind;
            Column column = null;
            KeyKind keyKind = KeyKind.LONG;
            DocValueFormat format = null;
            long dateInterval = 0L;
            Rounding rounding = null;
            List<String> branchKeys = null;
            RangeAggregator.Range[] ranges = null;
            String otherBucketKey = null;
            if (current instanceof FilterAggregationBuilder filter) {
                kind = LevelKind.FILTER;
                if (!FilterChecks.supported(filter.getFilter(), schema, multiFields, qsc)) {
                    return null;
                }
                branchKeys = List.of(filter.getName());
                estimatedGroups = saturatingMultiply(estimatedGroups, 2L);
            } else if (current instanceof FiltersAggregationBuilder filters) {
                kind = LevelKind.FILTERS;
                List<String> keys = new ArrayList<>(filters.filters().size());
                for (FiltersAggregator.KeyedFilter keyed : filters.filters()) {
                    if (!FilterChecks.supported(keyed.filter(), schema, multiFields, qsc)) {
                        return null;
                    }
                    keys.add(keyed.key());
                }
                branchKeys = keys;
                otherBucketKey = filters.otherBucket() ? filters.otherBucketKey() : null;
                estimatedGroups = saturatingMultiply(estimatedGroups, keys.size() + 1L);
            } else {
                ValuesSourceAggregationBuilder<?> bucketBuilder = (ValuesSourceAggregationBuilder<?>) current;
                column = resolveColumn(bucketBuilder.field(), schema, multiFields, qsc);
                if (column == null) {
                    return null;
                }
                format = column.fieldType().docValueFormat(bucketBuilder.format(), bucketBuilder.timeZone());
                if (bucketBuilder instanceof TermsAggregationBuilder terms) {
                    kind = LevelKind.TERMS;
                    if (column.isUtf8()) {
                        keyKind = KeyKind.STRING;
                    } else if (column.isFloating()) {
                        keyKind = KeyKind.DOUBLE;
                    }
                    // Every terms level multiplies the combinations the
                    // scan may return by the groups this level keeps.
                    estimatedGroups = saturatingMultiply(estimatedGroups, thresholds(terms).getShardSize());
                } else if (bucketBuilder instanceof HistogramAggregationBuilder) {
                    kind = LevelKind.HISTOGRAM;
                    if (column.isUtf8() || column.isDate() || column.isBoolean()) {
                        return null;
                    }
                } else if (bucketBuilder instanceof DateHistogramAggregationBuilder dateHistogram) {
                    kind = LevelKind.DATE_HISTOGRAM;
                    if (!column.isDate()) {
                        return null;
                    }
                    String calendarUnit = LanceAggregationSupport.calendarUnit(dateHistogram);
                    if (calendarUnit != null) {
                        // date_trunc truncates in the column's zone, which
                        // has to be UTC to match the aggregator's rounding
                        // for a request without time_zone. The key is the
                        // bucket start in millis, so no interval is
                        // multiplied back.
                        if (!column.isUtcTimestamp()) {
                            return null;
                        }
                        rounding = Rounding.builder(
                            DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(dateHistogram.getCalendarInterval().toString())
                        ).build();
                    } else {
                        dateInterval = fixedIntervalMillisOrZero(dateHistogram.getFixedInterval().toString());
                        if (dateInterval <= 0L) {
                            return null;
                        }
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
                    List<String> keys = new ArrayList<>(ranges.length);
                    for (RangeAggregator.Range range : ranges) {
                        keys.add(range.getKey());
                    }
                    branchKeys = keys;
                    estimatedGroups = saturatingMultiply(estimatedGroups, ranges.length + 1L);
                } else {
                    kind = LevelKind.MISSING;
                    branchKeys = List.of(bucketBuilder.getName());
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
            if (kind.isMask()) {
                levels.add(
                    new Level(current, kind, column, KeyKind.LONG, format, children, metrics, 0L, null, branchKeys, ranges, otherBucketKey)
                );
            } else {
                levels.add(new Level(current, kind, column, keyKind, format, children, metrics, dateInterval, rounding, null, null, null));
            }
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
        return new ResolvedAggregate(shape, substrait, levels, null, List.of(), allMetrics, bins, topK);
    }

    /**
     * Validation-only mirror of the mask predicates the translator
     * spelled into the pushed aggregate: whether the query of a
     * {@code filter} / {@code filters} bucket resolves against the
     * mapping the way the hand written pushdown resolved it. The
     * translator resolves fields against the Arrow schema alone (the
     * coordinating node has no mapping), so a query it accepted may
     * still name a field the mapping types differently, a {@code text}
     * column being the case the Arrow type cannot show; the executor
     * refuses those here and the request stays on the aggregators, as
     * before.
     */
    private static final class FilterChecks {

        private FilterChecks() {}

        static boolean supported(
            QueryBuilder query,
            Schema schema,
            Map<String, LinkedHashMap<String, String>> multiFields,
            QueryShardContext qsc
        ) {
            if (query == null || query instanceof MatchAllQueryBuilder) {
                return true;
            }
            if (query instanceof TermQueryBuilder term) {
                Column column = resolveColumn(term.fieldName(), schema, multiFields, qsc);
                return column != null && valueComparable(column, term.value(), qsc);
            }
            if (query instanceof TermsQueryBuilder terms) {
                Column column = resolveColumn(terms.fieldName(), schema, multiFields, qsc);
                if (column == null) {
                    return false;
                }
                if (terms.values() == null || terms.values().isEmpty()) {
                    return true;
                }
                for (Object value : terms.values()) {
                    if (!valueComparable(column, value, qsc)) {
                        return false;
                    }
                }
                return true;
            }
            if (query instanceof ExistsQueryBuilder exists) {
                return resolveColumn(exists.fieldName(), schema, multiFields, qsc) != null;
            }
            if (query instanceof RangeQueryBuilder range) {
                Column column = resolveColumn(range.fieldName(), schema, multiFields, qsc);
                return column != null && rangeComparable(column, range, qsc);
            }
            if (query instanceof BoolQueryBuilder bool) {
                if (bool.minimumShouldMatch() != null) {
                    return false;
                }
                boolean positive = !bool.must().isEmpty() || !bool.filter().isEmpty();
                if (!positive && bool.should().isEmpty() && !bool.mustNot().isEmpty() && !bool.adjustPureNegative()) {
                    // A purely negative BooleanQuery matches nothing
                    // unless the builder adds the match_all it does by
                    // default; the pushdown never spells that.
                    return false;
                }
                for (QueryBuilder clause : bool.must()) {
                    if (!supported(clause, schema, multiFields, qsc)) {
                        return false;
                    }
                }
                for (QueryBuilder clause : bool.filter()) {
                    if (!supported(clause, schema, multiFields, qsc)) {
                        return false;
                    }
                }
                if (!positive) {
                    for (QueryBuilder clause : bool.should()) {
                        if (!supported(clause, schema, multiFields, qsc)) {
                            return false;
                        }
                    }
                }
                for (QueryBuilder clause : bool.mustNot()) {
                    if (!supported(clause, schema, multiFields, qsc)) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        /** Whether {@code column = value} resolves in the column's type, as the pushdown's term predicate required. */
        private static boolean valueComparable(Column column, Object value, QueryShardContext qsc) {
            if (value == null) {
                return false;
            }
            if (column.isUtf8()) {
                return true;
            }
            if (column.isDate()) {
                return parseDate(column, null, value, false, qsc) != null && parseDate(column, null, value, true, qsc) != null;
            }
            if (column.isBoolean()) {
                return bool(value) != null;
            }
            if (column.isFloating()) {
                return floating(value) != null;
            }
            return integral(value) != null;
        }

        /** Whether the range query's bounds resolve, as the pushdown's range predicate required. */
        private static boolean rangeComparable(Column column, RangeQueryBuilder range, QueryShardContext qsc) {
            if (column.isUtf8() || column.isBoolean() || (range.from() == null && range.to() == null)) {
                return false;
            }
            if (column.isDate()) {
                if (range.from() != null && parseDate(column, range, range.from(), !range.includeLower(), qsc) == null) {
                    return false;
                }
                return range.to() == null || parseDate(column, range, range.to(), range.includeUpper(), qsc) != null;
            }
            if (column.isFloating()) {
                if (range.from() != null && floating(range.from()) == null) {
                    return false;
                }
                return range.to() == null || floating(range.to()) != null;
            }
            if (range.from() != null && integral(range.from()) == null) {
                return false;
            }
            return range.to() == null || integral(range.to()) != null;
        }

        /**
         * Epoch millis of a date bound through the field's date format
         * (the range query's {@code format} and {@code time_zone} when
         * it names them), null when the text does not parse.
         */
        private static Long parseDate(Column column, RangeQueryBuilder range, Object value, boolean roundUp, QueryShardContext qsc) {
            try {
                String pattern = range == null ? null : range.format();
                java.time.ZoneId zone = range == null || range.timeZone() == null ? null : java.time.ZoneId.of(range.timeZone());
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

        /** The value as a double, or null when it is not a number. */
        private static Double floating(Object value) {
            if (value instanceof Boolean) {
                return null;
            }
            if (value instanceof Number n) {
                return Double.isNaN(n.doubleValue()) ? null : n.doubleValue();
            }
            try {
                double number = Double.parseDouble(text(value));
                return Double.isNaN(number) ? null : number;
            } catch (NumberFormatException unparseable) {
                return null;
            }
        }
    }

    /**
     * The executor state of a composite tree: one source per composite
     * source with its parsed {@code after} value, format and direction,
     * and the metric children. Null when a field does not resolve, the
     * structure is outside the pushdown's composite subset, or an
     * {@code after} value is not of the type the source parses.
     */
    private static ResolvedAggregate resolveCompositeShape(
        PushedShape shape,
        ByteBuffer substrait,
        CompositeAggregationBuilder compositeBuilder,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        int bins
    ) {
        if (!LanceAggregationSupport.isPushdownComposite(compositeBuilder)) {
            return null;
        }
        Map<String, Object> after = afterKey(compositeBuilder);
        List<Source> sources = new ArrayList<>(compositeBuilder.sources().size());
        for (CompositeValuesSourceBuilder<?> sourceBuilder : compositeBuilder.sources()) {
            Column column = resolveColumn(sourceBuilder.field(), schema, multiFields, qsc);
            if (column == null) {
                return null;
            }
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
                keyKind = KeyKind.LONG;
                // The composite date source keeps the key as raw millis
                // unless the request names a format.
                format = sourceBuilder.format() == null
                    ? DocValueFormat.RAW
                    : column.fieldType().docValueFormat(sourceBuilder.format(), dateHistogram.timeZone());
            } else {
                if (column.isUtf8()) {
                    keyKind = KeyKind.STRING;
                } else if (column.isFloating()) {
                    keyKind = KeyKind.DOUBLE;
                } else {
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
            sources.add(new Source(sourceBuilder, column, keyKind, format, reverseMul, afterValue, dateInterval));
        }
        List<Metric> allMetrics = new ArrayList<>();
        List<Metric> metrics = resolveMetrics(new ArrayList<>(compositeBuilder.getSubAggregations()), schema, multiFields, qsc, allMetrics);
        if (metrics == null) {
            return null;
        }
        Composite composite = new Composite(compositeBuilder, sources, metrics);
        return new ResolvedAggregate(shape, substrait, List.of(), composite, List.of(), allMetrics, bins, null);
    }
}
