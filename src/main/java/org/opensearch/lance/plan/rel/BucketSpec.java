/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * The OpenSearch bucket semantics of one group key of a
 * {@link LanceAggregate}: everything the request's bucket aggregation
 * carries that Calcite's {@code Aggregate} does not model. The group
 * key expression itself (a field reference, a floored division, a
 * {@code CASE WHEN} bit mask) lives in the projection under the
 * aggregate; this record carries the request options the executor needs
 * to rebuild the OpenSearch response from the grouped rows.
 *
 * <p>Only the fields a {@link Kind} uses are set; the rest are null.
 * {@code offset} and {@code timeZone} are carried for completeness but
 * are always 0 / null today: the translator refuses a request that sets
 * them.
 *
 * @param kind how the key opens buckets
 * @param aggregationName the request's name for the bucket aggregation
 * @param size terms {@code size}, or the composite {@code size}
 * @param shardSize terms {@code shard_size} as the aggregator factory
 *     derives it (the distributed counting heuristic applied when the
 *     request named none)
 * @param minDocCount terms / histogram {@code min_doc_count}
 * @param order terms bucket order
 * @param interval histogram interval
 * @param dateIntervalMillis fixed date_histogram interval, or the
 *     fixed length of a composite date source
 * @param calendarUnit calendar date_histogram unit ({@code second} ..
 *     {@code year})
 * @param offset histogram / date_histogram offset (always 0 today)
 * @param timeZone date_histogram time zone (always null today)
 * @param format the request's key format pattern
 * @param ranges range / date_range bounds, in the sorted order the
 *     aggregator prepares them
 * @param filterKeys the bucket keys of a filter / filters aggregation;
 *     empty for {@code missing}, whose one branch is named by
 *     {@code aggregationName}
 * @param otherBucketKey the filters other bucket key, null when the
 *     request asked for none
 * @param compositeAfter the request's raw {@code after} key, carried on
 *     every composite source spec of the request
 * @param compositeSourceOrder {@code ASC} / {@code DESC} of a composite
 *     source
 * @param compositeSize the composite {@code size}, on every source spec
 * @param missingBucket composite source {@code missing_bucket}; always
 *     false today, the translator refuses it explicitly
 */
public record BucketSpec(Kind kind, String aggregationName, Integer size, Integer shardSize, Long minDocCount, OrderSpec order,
    Double interval, Long dateIntervalMillis, String calendarUnit, Long offset, String timeZone, String format, List<RangeSpec> ranges,
    List<String> filterKeys, String otherBucketKey, Map<String, Object> compositeAfter, String compositeSourceOrder, Integer compositeSize,
    boolean missingBucket) {

    /** The bucket aggregation kinds the planner models. */
    public enum Kind {
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

    /**
     * A terms bucket order.
     *
     * @param kind the order kind
     * @param path the sub aggregation path of a
     *     {@link Kind#SUB_AGGREGATION} order, else null
     * @param ascending the direction of a sub aggregation order
     */
    public record OrderSpec(Kind kind, String path, boolean ascending) {

        /** The terms order kinds. */
        public enum Kind {
            COUNT_DESC,
            COUNT_ASC,
            KEY_ASC,
            KEY_DESC,
            SUB_AGGREGATION
        }

        /** An order on the bucket count or the key. */
        public static OrderSpec of(Kind kind) {
            return new OrderSpec(kind, null, false);
        }

        /** An order on one sub aggregation's value. */
        public static OrderSpec subAggregation(String path, boolean ascending) {
            return new OrderSpec(Kind.SUB_AGGREGATION, Objects.requireNonNull(path), ascending);
        }

        @Override
        public String toString() {
            return kind == Kind.SUB_AGGREGATION ? kind + "(" + path + ", " + (ascending ? "asc" : "desc") + ")" : kind.toString();
        }
    }

    /**
     * One range of a range / date_range aggregation.
     *
     * @param key the bucket key
     * @param from the resolved lower bound (a {@code Double}, or the
     *     parsed date millis as a {@code Long}), null for an open end
     * @param to the resolved upper bound, null for an open end
     */
    public record RangeSpec(String key, Object from, Object to) {
        @Override
        public String toString() {
            return "(" + key + ", " + from + ", " + to + ")";
        }
    }

    /** A terms bucket. */
    public static BucketSpec terms(String name, int size, int shardSize, long minDocCount, OrderSpec order, String format) {
        return new BucketSpec(
            Kind.TERMS,
            name,
            size,
            shardSize,
            minDocCount,
            order,
            null,
            null,
            null,
            null,
            null,
            format,
            null,
            null,
            null,
            null,
            null,
            null,
            false
        );
    }

    /** A numeric histogram bucket. */
    public static BucketSpec histogram(String name, double interval, long minDocCount, String format) {
        return new BucketSpec(
            Kind.HISTOGRAM,
            name,
            null,
            null,
            minDocCount,
            null,
            interval,
            null,
            null,
            0L,
            null,
            format,
            null,
            null,
            null,
            null,
            null,
            null,
            false
        );
    }

    /** A fixed interval date_histogram bucket. */
    public static BucketSpec dateHistogramFixed(String name, long intervalMillis, long minDocCount, String format) {
        return new BucketSpec(
            Kind.DATE_HISTOGRAM_FIXED,
            name,
            null,
            null,
            minDocCount,
            null,
            null,
            intervalMillis,
            null,
            0L,
            null,
            format,
            null,
            null,
            null,
            null,
            null,
            null,
            false
        );
    }

    /** A calendar interval date_histogram bucket. */
    public static BucketSpec dateHistogramCalendar(String name, String unit, long minDocCount, String format) {
        return new BucketSpec(
            Kind.DATE_HISTOGRAM_CALENDAR,
            name,
            null,
            null,
            minDocCount,
            null,
            null,
            null,
            unit,
            0L,
            null,
            format,
            null,
            null,
            null,
            null,
            null,
            null,
            false
        );
    }

    /** A range or date_range bucket. */
    public static BucketSpec ranges(Kind kind, String name, List<RangeSpec> ranges, String format) {
        return new BucketSpec(
            kind,
            name,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            format,
            List.copyOf(ranges),
            null,
            null,
            null,
            null,
            null,
            false
        );
    }

    /** A filter or filters bucket. */
    public static BucketSpec filters(Kind kind, String name, List<String> filterKeys, String otherBucketKey) {
        return new BucketSpec(
            kind,
            name,
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
            List.copyOf(filterKeys),
            otherBucketKey,
            null,
            null,
            null,
            false
        );
    }

    /** A missing bucket; its one branch is named by {@link #aggregationName()}, so {@code filterKeys} stays empty. */
    public static BucketSpec missing(String name) {
        return new BucketSpec(
            Kind.MISSING,
            name,
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
            List.of(),
            null,
            null,
            null,
            null,
            false
        );
    }

    /** A composite terms source. */
    public static BucketSpec compositeTerms(String name, String sourceOrder, Map<String, Object> after, int compositeSize, String format) {
        return new BucketSpec(
            Kind.COMPOSITE_TERMS,
            name,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            format,
            null,
            null,
            null,
            after,
            sourceOrder,
            compositeSize,
            false
        );
    }

    /** A composite date_histogram source with a fixed length interval. */
    public static BucketSpec compositeDateHistogram(
        String name,
        long intervalMillis,
        String sourceOrder,
        Map<String, Object> after,
        int compositeSize,
        String format
    ) {
        return new BucketSpec(
            Kind.COMPOSITE_DATE_HISTOGRAM,
            name,
            null,
            null,
            null,
            null,
            null,
            intervalMillis,
            null,
            null,
            null,
            format,
            null,
            null,
            null,
            after,
            sourceOrder,
            compositeSize,
            false
        );
    }

    /**
     * Prints the kind and every set field, so the explain output shows
     * the full OpenSearch shape of the bucket.
     */
    @Override
    public String toString() {
        StringJoiner fields = new StringJoiner(", ", kind + "{", "}");
        fields.add("name=" + aggregationName);
        if (size != null) {
            fields.add("size=" + size);
        }
        if (shardSize != null) {
            fields.add("shardSize=" + shardSize);
        }
        if (minDocCount != null) {
            fields.add("minDocCount=" + minDocCount);
        }
        if (order != null) {
            fields.add("order=" + order);
        }
        if (interval != null) {
            fields.add("interval=" + interval);
        }
        if (dateIntervalMillis != null) {
            fields.add("intervalMillis=" + dateIntervalMillis);
        }
        if (calendarUnit != null) {
            fields.add("unit=" + calendarUnit);
        }
        if (offset != null && offset != 0L) {
            fields.add("offset=" + offset);
        }
        if (timeZone != null) {
            fields.add("timeZone=" + timeZone);
        }
        if (format != null) {
            fields.add("format=" + format);
        }
        if (ranges != null) {
            fields.add("ranges=" + ranges);
        }
        if (filterKeys != null && !filterKeys.isEmpty()) {
            fields.add("keys=" + filterKeys);
        }
        if (otherBucketKey != null) {
            fields.add("otherBucketKey=" + otherBucketKey);
        }
        if (compositeAfter != null) {
            fields.add("after=" + compositeAfter);
        }
        if (compositeSourceOrder != null) {
            fields.add("sourceOrder=" + compositeSourceOrder);
        }
        if (compositeSize != null) {
            fields.add("compositeSize=" + compositeSize);
        }
        if (missingBucket) {
            fields.add("missingBucket=true");
        }
        return fields.toString();
    }
}
