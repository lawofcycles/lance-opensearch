/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.QueryShardContext;
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
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
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
 * {@code min}, {@code max}, {@code value_count}), or one bucket
 * aggregation ({@code terms}, {@code histogram}, fixed interval
 * {@code date_histogram}) with metric children. The structural rules
 * live in {@link LanceAggregationSupport#isPushdownCandidate}; this
 * class adds the field checks: every field is mapped, backed by a
 * scalar Lance column of a matching Arrow type ({@code keyword} on
 * {@code Utf8}, integer types on signed {@code Int}, {@code float} /
 * {@code double} on {@code FloatingPoint}, {@code boolean} on
 * {@code Bool}, {@code date} on {@code Date} / {@code Timestamp}), and
 * a keyword sub-field resolves to its base column. {@code List<Utf8>}
 * columns are refused because a group by on the list would count rows,
 * not elements.
 *
 * <p>Semantics reproduced from the shard aggregators:
 * <ul>
 *   <li>Rows whose bucket key is null form no bucket (the aggregators
 *       skip documents without a value) but still count toward
 *       {@code hits.total}, which is the sum of {@code count(*)} over
 *       every group.</li>
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
 *       on doubles, {@code date_histogram} keys are
 *       {@code floorDiv(millis, interval) * interval} on longs, both
 *       sorted ascending; empty bucket filling for {@code min_doc_count}
 *       0 stays with the coordinator's reduce, which reads the
 *       {@code EmptyBucketInfo} attached here.</li>
 *   <li>Date columns are converted to epoch milliseconds inside the
 *       scan with the leaf reader's conversion, booleans to 0 / 1, so
 *       {@code sum} / {@code min} / {@code max} on them return the
 *       same numbers the doc values path returns.</li>
 * </ul>
 */
final class LanceAggregatePushdown {

    private LanceAggregatePushdown() {}

    /** Output column of the per group row count. */
    private static final String COUNT_COLUMN = "n";
    /** Output column of the bucket key. */
    private static final String KEY_COLUMN = "k";

    /** Aggregations plus the row total of the fragments this node scanned. */
    record Result(InternalAggregations aggregations, long totalRows) {
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
     * One metric aggregation: its measures occupy the result columns
     * {@code prefix} ({@code sum} / {@code min} / {@code max} /
     * {@code value_count}) or {@code prefix + "_s"} and
     * {@code prefix + "_c"} ({@code avg}, whose sum and count travel
     * separately so the coordinator can merge averages).
     */
    private record Metric(String name, MetricKind kind, Column column, DocValueFormat format, Map<String, Object> metadata, String prefix) {

        void addMeasures(SubstraitAggregatePlan.Builder builder) {
            Expression value = column.numericExpression();
            ScalarType type = column.isFloating() ? ScalarType.FP64 : ScalarType.I64;
            switch (kind) {
                case SUM -> builder.measure("sum", List.of(value), type, prefix);
                case MIN -> builder.measure("min", List.of(value), type, prefix);
                case MAX -> builder.measure("max", List.of(value), type, prefix);
                case VALUE_COUNT -> builder.measure("count", List.of(new FieldReference(column.index())), ScalarType.I64, prefix);
                case AVG -> {
                    builder.measure("sum", List.of(value), type, prefix + "_s");
                    builder.measure("count", List.of(new FieldReference(column.index())), ScalarType.I64, prefix + "_c");
                }
            }
        }

        InternalAggregation read(VectorSchemaRoot root, int row) {
            return switch (kind) {
                case SUM -> new InternalSum(name, doubleOrZero(root.getVector(prefix), row), format, metadata);
                case MIN -> new InternalMin(name, doubleOr(root.getVector(prefix), row, Double.POSITIVE_INFINITY), format, metadata);
                case MAX -> new InternalMax(name, doubleOr(root.getVector(prefix), row, Double.NEGATIVE_INFINITY), format, metadata);
                case VALUE_COUNT -> new InternalValueCount(name, longOrZero(root.getVector(prefix), row), metadata);
                case AVG -> new InternalAvg(
                    name,
                    doubleOrZero(root.getVector(prefix + "_s"), row),
                    longOrZero(root.getVector(prefix + "_c"), row),
                    format,
                    metadata
                );
            };
        }

        /** The value the aggregator reports for a bucket that saw no document. */
        InternalAggregation empty() {
            return switch (kind) {
                case SUM -> new InternalSum(name, 0d, format, metadata);
                case MIN -> new InternalMin(name, Double.POSITIVE_INFINITY, format, metadata);
                case MAX -> new InternalMax(name, Double.NEGATIVE_INFINITY, format, metadata);
                case VALUE_COUNT -> new InternalValueCount(name, 0L, metadata);
                case AVG -> new InternalAvg(name, 0d, 0L, format, metadata);
            };
        }
    }

    /** The bucket aggregation, or {@code null} for a metrics only tree. */
    private record Bucket(ValuesSourceAggregationBuilder<?> builder, Column column, KeyKind keyKind, DocValueFormat format, List<
        Metric> metrics) {
    }

    /** One group row read from Lance, kept until the terms selection has picked the buckets to build. */
    private record Group(Object key, long count, InternalAggregations aggregations) {
    }

    /**
     * An encoded plan ready to run. Created by {@link #plan}; a
     * {@code null} plan means the request takes the aggregator path.
     */
    static final class Plan {
        private final ByteBuffer substrait;
        private final Bucket bucket;
        private final List<Metric> topMetrics;

        private Plan(ByteBuffer substrait, Bucket bucket, List<Metric> topMetrics) {
            this.substrait = substrait;
            this.bucket = bucket;
            this.topMetrics = topMetrics;
        }

        /** True when {@link #execute} needs the {@code date_histogram} prototype from the aggregator. */
        boolean needsDateHistogramPrototype() {
            return bucket != null && bucket.builder() instanceof DateHistogramAggregationBuilder;
        }

        /** Name of the bucket aggregation, when there is one. */
        String bucketName() {
            return bucket == null ? null : bucket.builder().getName();
        }

        /**
         * Runs the plan. {@code fragmentIds} null means every fragment;
         * {@code filterSql} null means no filter. {@code dateHistogramPrototype}
         * supplies, for the bucket aggregation name, the empty
         * {@link InternalDateHistogram} the aggregator would build: its
         * constructor is package private, so the buckets are attached
         * through its public {@code create(List)} instead. Only consulted
         * when {@link #needsDateHistogramPrototype()} is true.
         *
         * <p>Failures inside Lance (a plan it cannot parse, a function
         * its DataFusion build lacks) propagate: falling back to the
         * aggregator path would hide the regression behind a slow answer.
         */
        Result execute(
            Dataset dataset,
            List<Integer> fragmentIds,
            String filterSql,
            Function<String, InternalAggregation> dateHistogramPrototype
        ) throws Exception {
            ScanOptions.Builder options = new ScanOptions.Builder().substraitAggregate(substrait.duplicate());
            if (fragmentIds != null) {
                options.fragmentIds(fragmentIds);
            }
            if (filterSql != null) {
                options.filter(filterSql);
            }
            long total = 0L;
            List<Group> groups = new ArrayList<>();
            InternalAggregations metricsOnly = null;
            try (LanceScanner scanner = dataset.newScan(options.build()); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    FieldVector counts = root.getVector(COUNT_COLUMN);
                    for (int row = 0; row < root.getRowCount(); row++) {
                        long count = longOrZero(counts, row);
                        total += count;
                        if (bucket == null) {
                            metricsOnly = readMetrics(topMetrics, root, row);
                            continue;
                        }
                        FieldVector keys = root.getVector(KEY_COLUMN);
                        if (keys.isNull(row)) {
                            // Documents without a value open no bucket.
                            continue;
                        }
                        groups.add(new Group(key(keys, row), count, readMetrics(bucket.metrics(), root, row)));
                    }
                }
            }
            if (bucket == null) {
                // Lance returns exactly one row for a plan without
                // groupings, even over zero fragments; the guard only
                // covers a reader that yielded no batch at all.
                if (metricsOnly == null) {
                    List<InternalAggregation> empties = new ArrayList<>(topMetrics.size());
                    for (Metric metric : topMetrics) {
                        empties.add(metric.empty());
                    }
                    metricsOnly = InternalAggregations.from(empties);
                }
                return new Result(metricsOnly, total);
            }
            InternalAggregation aggregation;
            if (bucket.builder() instanceof TermsAggregationBuilder terms) {
                aggregation = buildTerms(terms, groups);
            } else if (bucket.builder() instanceof HistogramAggregationBuilder histogram) {
                aggregation = buildHistogram(histogram, groups);
            } else {
                DateHistogramAggregationBuilder dateHistogram = (DateHistogramAggregationBuilder) bucket.builder();
                InternalAggregation prototype = dateHistogramPrototype.apply(dateHistogram.getName());
                aggregation = buildDateHistogram(dateHistogram, (InternalDateHistogram) prototype, groups);
            }
            return new Result(InternalAggregations.from(Collections.singletonList(aggregation)), total);
        }

        private InternalAggregation buildTerms(TermsAggregationBuilder terms, List<Group> groups) {
            BucketOrder order = terms.order();
            TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(
                terms.minDocCount(),
                terms.shardMinDocCount(),
                terms.size(),
                terms.shardSize()
            );
            // -1 is the builder's "not set" shard_size; the factory then
            // applies the distributed counting heuristic for non key orders.
            if (!InternalOrder.isKeyOrder(order) && thresholds.getShardSize() == -1) {
                thresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(thresholds.getRequiredSize()));
            }
            thresholds.ensureValidity();
            int shardSize = thresholds.getShardSize();

            // Keep the first shard_size groups in request order. The
            // queue's top is the group that leaves first, so lessThan is
            // "sorts later".
            Comparator<Group> groupOrder = groupComparator(order, bucket.keyKind());
            PriorityQueue<Group> queue = new PriorityQueue<>(Math.max(1, Math.min(shardSize, Math.max(1, groups.size())))) {
                @Override
                protected boolean lessThan(Group a, Group b) {
                    return groupOrder.compare(a, b) > 0;
                }
            };
            long otherDocCount = 0L;
            for (Group group : groups) {
                otherDocCount += group.count();
                queue.insertWithOverflow(group);
            }
            List<Group> selected = new ArrayList<>(queue.size());
            while (queue.size() > 0) {
                selected.add(queue.pop());
            }
            Collections.reverse(selected);
            for (Group group : selected) {
                otherDocCount -= group.count();
            }
            // Shards hand the reduce key sorted buckets unless the
            // request order is itself a key order.
            BucketOrder reduceOrder;
            if (InternalOrder.isKeyOrder(order)) {
                reduceOrder = order;
            } else {
                reduceOrder = BucketOrder.key(true);
                selected.sort(groupComparator(reduceOrder, bucket.keyKind()));
            }
            boolean showError = terms.showTermDocCountError();
            String name = terms.getName();
            Map<String, Object> metadata = metadata(terms);
            DocValueFormat format = bucket.format();
            switch (bucket.keyKind()) {
                case STRING -> {
                    List<StringTerms.Bucket> buckets = new ArrayList<>(selected.size());
                    for (Group group : selected) {
                        buckets.add(
                            new StringTerms.Bucket((BytesRef) group.key(), group.count(), group.aggregations(), showError, 0L, format)
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
                    for (Group group : selected) {
                        buckets.add(new LongTerms.Bucket((Long) group.key(), group.count(), group.aggregations(), showError, 0L, format));
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
                    for (Group group : selected) {
                        buckets.add(
                            new DoubleTerms.Bucket((Double) group.key(), group.count(), group.aggregations(), showError, 0L, format)
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
                default -> throw new IllegalStateException("unexpected key kind " + bucket.keyKind());
            }
        }

        private InternalAggregation buildHistogram(HistogramAggregationBuilder histogram, List<Group> groups) {
            double interval = histogram.interval();
            double offset = histogram.offset();
            groups.sort(Comparator.comparingLong(group -> (Long) group.key()));
            List<InternalHistogram.Bucket> buckets = new ArrayList<>(groups.size());
            for (Group group : groups) {
                // The scan returns floor((value - offset) / interval); the
                // aggregator multiplies back the same way.
                double key = (double) (Long) group.key() * interval + offset;
                buckets.add(new InternalHistogram.Bucket(key, group.count(), histogram.keyed(), bucket.format(), group.aggregations()));
            }
            InternalHistogram.EmptyBucketInfo emptyBucketInfo = null;
            if (histogram.minDocCount() == 0L) {
                emptyBucketInfo = new InternalHistogram.EmptyBucketInfo(
                    interval,
                    offset,
                    histogram.minBound(),
                    histogram.maxBound(),
                    emptyMetrics(bucket.metrics())
                );
            }
            return new InternalHistogram(
                histogram.getName(),
                buckets,
                histogram.order(),
                histogram.minDocCount(),
                emptyBucketInfo,
                bucket.format(),
                histogram.keyed(),
                metadata(histogram)
            );
        }

        private InternalAggregation buildDateHistogram(
            DateHistogramAggregationBuilder dateHistogram,
            InternalDateHistogram prototype,
            List<Group> groups
        ) {
            long interval = fixedIntervalMillis(dateHistogram);
            groups.sort(Comparator.comparingLong(group -> (Long) group.key()));
            List<InternalDateHistogram.Bucket> buckets = new ArrayList<>(groups.size());
            for (Group group : groups) {
                long key = (Long) group.key() * interval;
                buckets.add(
                    new InternalDateHistogram.Bucket(key, group.count(), dateHistogram.keyed(), bucket.format(), group.aggregations())
                );
            }
            // The prototype carries the rounding, offset, order,
            // min_doc_count, empty bucket info and format the aggregator
            // computed for this request; create() copies them.
            return prototype.create(buckets);
        }
    }

    /**
     * Resolves the request's aggregation tree against the table schema
     * and the index mapping. Returns {@code null} when any part of the
     * tree is outside what the scan can compute, in which case the
     * caller runs the Lucene aggregators.
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
        if (!LanceAggregationSupport.isPushdownCandidate(aggregations)) {
            return null;
        }
        List<AggregationBuilder> top = new ArrayList<>(aggregations.getAggregatorFactories());
        SubstraitAggregatePlan.Builder builder = new SubstraitAggregatePlan.Builder();
        if (LanceAggregationSupport.isPushdownMetric(top.get(0))) {
            List<Metric> metrics = resolveMetrics(top, schema, multiFields, qsc);
            if (metrics == null) {
                return null;
            }
            builder.measure("count", List.of(), ScalarType.I64, COUNT_COLUMN);
            for (Metric metric : metrics) {
                metric.addMeasures(builder);
            }
            return new Plan(builder.build(), null, metrics);
        }
        ValuesSourceAggregationBuilder<?> bucketBuilder = (ValuesSourceAggregationBuilder<?>) top.get(0);
        Column column = resolveColumn(bucketBuilder.field(), schema, multiFields, qsc);
        if (column == null) {
            return null;
        }
        Expression keyExpression;
        KeyKind keyKind;
        if (bucketBuilder instanceof TermsAggregationBuilder) {
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
        } else if (bucketBuilder instanceof HistogramAggregationBuilder histogram) {
            if (column.isUtf8() || column.isDate() || column.isBoolean()) {
                return null;
            }
            keyExpression = SubstraitExpressions.floorFp64(new FieldReference(column.index()), histogram.offset(), histogram.interval());
            keyKind = KeyKind.LONG;
        } else {
            DateHistogramAggregationBuilder dateHistogram = (DateHistogramAggregationBuilder) bucketBuilder;
            if (!column.isDate()) {
                return null;
            }
            long interval;
            try {
                interval = fixedIntervalMillis(dateHistogram);
            } catch (IllegalArgumentException unparseable) {
                return null;
            }
            if (interval <= 0L) {
                return null;
            }
            keyExpression = SubstraitExpressions.floorDivInt64(column.numericExpression(), dateHistogram.offset(), interval);
            keyKind = KeyKind.LONG;
        }
        List<Metric> metrics = resolveMetrics(new ArrayList<>(bucketBuilder.getSubAggregations()), schema, multiFields, qsc);
        if (metrics == null) {
            return null;
        }
        builder.groupBy(keyExpression, KEY_COLUMN);
        builder.measure("count", List.of(), ScalarType.I64, COUNT_COLUMN);
        for (Metric metric : metrics) {
            metric.addMeasures(builder);
        }
        DocValueFormat format = column.fieldType().docValueFormat(bucketBuilder.format(), bucketBuilder.timeZone());
        Bucket bucket = new Bucket(bucketBuilder, column, keyKind, format, metrics);
        return new Plan(builder.build(), bucket, Collections.emptyList());
    }

    private static List<Metric> resolveMetrics(
        List<AggregationBuilder> builders,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc
    ) {
        List<Metric> metrics = new ArrayList<>(builders.size());
        int index = 0;
        for (AggregationBuilder builder : builders) {
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
            metrics.add(new Metric(builder.getName(), kind, column, format, metadata(builder), "m" + index++));
        }
        return metrics;
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

    /** Milliseconds of the builder's {@code fixed_interval}, parsed the way the builder itself parses it. */
    private static long fixedIntervalMillis(DateHistogramAggregationBuilder dateHistogram) {
        return TimeValue.parseTimeValue(dateHistogram.getFixedInterval().toString(), null, "fixed_interval").getMillis();
    }

    private static InternalAggregations readMetrics(List<Metric> metrics, VectorSchemaRoot root, int row) {
        if (metrics.isEmpty()) {
            return InternalAggregations.EMPTY;
        }
        List<InternalAggregation> values = new ArrayList<>(metrics.size());
        for (Metric metric : metrics) {
            values.add(metric.read(root, row));
        }
        return InternalAggregations.from(values);
    }

    private static InternalAggregations emptyMetrics(List<Metric> metrics) {
        if (metrics.isEmpty()) {
            return InternalAggregations.EMPTY;
        }
        List<InternalAggregation> values = new ArrayList<>(metrics.size());
        for (Metric metric : metrics) {
            values.add(metric.empty());
        }
        return InternalAggregations.from(values);
    }

    /**
     * Ordering of groups for the terms selection: {@code _count}
     * descending with the key ascending as tie breaker, or the key in
     * the requested direction. Keys compare the way the terms buckets
     * compare them: {@link BytesRef} order for strings, numeric order
     * otherwise.
     */
    private static Comparator<Group> groupComparator(BucketOrder order, KeyKind keyKind) {
        Comparator<Group> byKey = switch (keyKind) {
            case STRING -> Comparator.comparing(group -> (BytesRef) group.key());
            case LONG -> Comparator.comparingLong(group -> (Long) group.key());
            case DOUBLE -> Comparator.comparingDouble(group -> (Double) group.key());
        };
        if (InternalOrder.isKeyOrder(order)) {
            return InternalOrder.isKeyAsc(order) ? byKey : byKey.reversed();
        }
        return Comparator.comparingLong(Group::count).reversed().thenComparing(byKey);
    }

    /** The group key as the object the bucket type expects: {@link BytesRef}, {@link Long} or {@link Double}. */
    private static Object key(FieldVector vector, int row) {
        if (vector instanceof VarCharVector v) {
            return new BytesRef(v.get(row));
        }
        if (vector instanceof Float4Vector v) {
            return (double) v.get(row);
        }
        if (vector instanceof Float8Vector v) {
            return v.get(row);
        }
        return asLong(vector, row);
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
