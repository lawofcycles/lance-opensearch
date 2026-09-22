/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.substrait;

import java.util.List;
import java.util.Map;

/**
 * View over the OpenSearch aggregation semantics a Calcite
 * {@code Aggregate} node cannot carry on its own. The planner's
 * aggregate node implements this interface so the Substrait producer
 * (and later the executor) can read, per group key, which bucket
 * aggregation produced the key expression and, per aggregate call,
 * which metric aggregation the call stands for.
 *
 * <p>Calcite's {@code Aggregate} vocabulary covers group keys and
 * standard aggregate functions only. OpenSearch shapes such as
 * {@code stats} (five measures behind one call), {@code cardinality}
 * (an extra grouping, no measure) or {@code percentiles} (a second
 * scan) need the original aggregation kind next to the call, and the
 * response builder needs names, sizes, orders and formats that play no
 * role in planning. The records here carry exactly those fields;
 * everything Calcite already models (the key expressions, the call
 * arguments) stays on the {@code Aggregate}.
 */
public interface LanceAggregateSpecs {

    /**
     * The bucket specification of one group key, aligned with the group
     * set order: index {@code i} describes the {@code i}-th set bit of
     * the aggregate's group set.
     */
    BucketSpec bucket(int groupKeyIndex);

    /**
     * The metric specification of one aggregate call, aligned with the
     * call order: index {@code i} describes
     * {@code getAggCallList().get(i)}.
     */
    MetricSpec metric(int callIndex);

    /** The bucket aggregation a group key stands for. */
    enum BucketKind {
        TERMS,
        HISTOGRAM,
        DATE_HISTOGRAM_FIXED,
        DATE_HISTOGRAM_CALENDAR,
        RANGE,
        DATE_RANGE,
        FILTER,
        FILTERS,
        MISSING,
        COMPOSITE_TERMS,
        COMPOSITE_DATE_HISTOGRAM
    }

    /** The metric aggregation an aggregate call stands for. */
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

    /** Sort direction of one composite source. */
    enum CompositeSourceOrder {
        ASC,
        DESC
    }

    /**
     * How a {@code terms} bucket orders its buckets. {@code path} and
     * {@code ascending} are meaningful for {@link Kind#SUB_AGGREGATION}
     * only; the other kinds carry their direction in the kind itself.
     *
     * @param kind      the order family
     * @param path      the sub aggregation path a
     *                  {@link Kind#SUB_AGGREGATION} order sorts by, null
     *                  otherwise
     * @param ascending sort direction of a {@link Kind#SUB_AGGREGATION}
     *                  order
     */
    record OrderSpec(Kind kind, String path, boolean ascending) {

        /** The order family. */
        public enum Kind {
            COUNT_DESC,
            COUNT_ASC,
            KEY_ASC,
            KEY_DESC,
            SUB_AGGREGATION
        }
    }

    /**
     * One range of a {@code range} / {@code date_range} bucket.
     * {@code from} and {@code to} are a {@code Double} or the parsed
     * date millis as a {@code Long}; null for an open end.
     */
    record RangeSpec(String key, Object from, Object to) {
    }

    /**
     * The request-side fields of one bucket aggregation. Fields apply
     * per {@link BucketKind}; a field another kind does not use is null
     * (or false for {@code missingBucket}).
     *
     * @param kind                 the bucket family
     * @param aggregationName      the request's name for the aggregation
     * @param size                 {@code terms} / composite page size
     * @param shardSize            {@code terms} shard_size
     * @param minDocCount          {@code terms} min_doc_count
     * @param order                {@code terms} order
     * @param interval             {@code histogram} interval
     * @param dateIntervalMillis   fixed {@code date_histogram} interval
     * @param calendarUnit         calendar {@code date_histogram} unit
     *                             ({@code second} ... {@code year})
     * @param offset               {@code histogram} / {@code date_histogram}
     *                             offset (millis for the date kinds)
     * @param timeZone             {@code date_histogram} time_zone
     * @param format               key format
     * @param ranges               {@code range} / {@code date_range} ranges
     * @param filterKeys           {@code filter} / {@code filters} bucket keys;
     *                             the predicates are {@code RexNode}s over the
     *                             input row type and live on the aggregate
     *                             node, in a list parallel to this one
     * @param otherBucketKey       {@code filters} other bucket key, null when
     *                             the request asks for none
     * @param compositeAfter       parsed {@code after} key of a composite
     *                             source
     * @param compositeSourceOrder sort direction of a composite source
     * @param compositeSize        composite page size
     * @param missingBucket        {@code missing_bucket} of a composite
     *                             source; carried so the translator can
     *                             refuse it explicitly
     */
    record BucketSpec(BucketKind kind, String aggregationName, Integer size, Integer shardSize, Long minDocCount, OrderSpec order,
        Double interval, Long dateIntervalMillis, String calendarUnit, Double offset, String timeZone, String format, List<
            RangeSpec> ranges, List<String> filterKeys, String otherBucketKey, Map<String, Object> compositeAfter,
        CompositeSourceOrder compositeSourceOrder, Integer compositeSize, boolean missingBucket) {

        /** A spec carrying the kind and name only, for kinds without extra request fields. */
        public static BucketSpec of(BucketKind kind, String aggregationName) {
            return new BucketSpec(
                kind,
                aggregationName,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                false
            );
        }
    }

    /**
     * The request-side fields of one metric aggregation.
     *
     * @param aggregationName    the request's name for the aggregation
     * @param kind               the metric family
     * @param percents           {@code percentiles} percents
     * @param values             {@code percentile_ranks} values
     * @param sigma              {@code extended_stats} sigma
     * @param precisionThreshold {@code cardinality} precision_threshold
     * @param keyed              {@code percentiles} keyed flag
     * @param format             value format
     */
    record MetricSpec(String aggregationName, MetricKind kind, double[] percents, double[] values, Double sigma, Long precisionThreshold,
        Boolean keyed, String format) {

        /** A spec carrying the kind and name only, for kinds without extra request fields. */
        public static MetricSpec of(MetricKind kind, String aggregationName) {
            return new MetricSpec(aggregationName, kind, null, null, null, null, null, null);
        }
    }
}
