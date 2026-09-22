/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel;

import java.util.Arrays;
import java.util.StringJoiner;

/**
 * The OpenSearch metric semantics of one aggregate call of a
 * {@link LanceAggregate}: the request's aggregation name and the
 * options Calcite's {@code AggregateCall} does not model. Only the
 * fields a {@link Kind} uses are set; the rest are null.
 *
 * <p>The two double arrays are treated as immutable by convention; the
 * record does not copy them. Their content participates in
 * {@link #toString()} (and therefore in the plan text) but not in
 * {@code equals}, which is fine for a value that lives inside a
 * {@code RelNode} compared by digest.
 *
 * @param aggregationName the request's name for the metric
 * @param kind the metric kind
 * @param percents the requested percentiles ({@code percentiles} only)
 * @param values the requested values ({@code percentile_ranks} only)
 * @param sigma the {@code extended_stats} sigma
 * @param precisionThreshold the cardinality {@code precision_threshold},
 *     null when the request named none
 * @param keyed the percentiles / percentile_ranks {@code keyed} flag
 * @param format the request's value format pattern
 */
public record MetricSpec(String aggregationName, Kind kind, double[] percents, double[] values, Double sigma, Long precisionThreshold,
    Boolean keyed, String format) {

    /** The metric aggregation kinds the planner models. */
    public enum Kind {
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

    /** A metric with no extra options beyond name, kind and format. */
    public static MetricSpec of(Kind kind, String name, String format) {
        return new MetricSpec(name, kind, null, null, null, null, null, format);
    }

    /**
     * Prints the kind and every set field, so the explain output shows
     * the full OpenSearch shape of the metric.
     */
    @Override
    public String toString() {
        StringJoiner fields = new StringJoiner(", ", kind + "{", "}");
        fields.add("name=" + aggregationName);
        if (percents != null) {
            fields.add("percents=" + Arrays.toString(percents));
        }
        if (values != null) {
            fields.add("values=" + Arrays.toString(values));
        }
        if (sigma != null) {
            fields.add("sigma=" + sigma);
        }
        if (precisionThreshold != null) {
            fields.add("precisionThreshold=" + precisionThreshold);
        }
        if (keyed != null) {
            fields.add("keyed=" + keyed);
        }
        if (format != null) {
            fields.add("format=" + format);
        }
        return fields.toString();
    }
}
