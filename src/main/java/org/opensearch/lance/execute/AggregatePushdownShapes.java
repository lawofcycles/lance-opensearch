/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.execute;

import java.io.IOException;

import org.opensearch.common.Rounding;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentileRanksAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesConfig;
import org.opensearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;

/**
 * The builder shapes the executor's {@link AggregateSpecResolver}
 * recognises when it folds a pushed aggregate's group rows back into
 * the request's aggregation tree: which builders are metrics the scan
 * computes, which {@code composite} sources it groups by, and the
 * readings of a {@code terms} order and a {@code date_histogram}
 * calendar unit the resolver needs. The coordinator's translator
 * ({@code AggregationToRel}) decides what is pushed; these predicates
 * let the executor check that the shipped tree is the one it can fold.
 */
final class AggregatePushdownShapes {

    private AggregatePushdownShapes() {}

    /**
     * A {@code composite} whose sources are all {@link #isPushdownCompositeSource}
     * and whose children are all metrics. {@code size} and {@code after}
     * are applied by the executor to the sorted group rows.
     */
    static boolean isPushdownComposite(CompositeAggregationBuilder composite) {
        if (composite.sources().isEmpty() || !composite.getPipelineAggregations().isEmpty()) {
            return false;
        }
        for (CompositeValuesSourceBuilder<?> source : composite.sources()) {
            if (!isPushdownCompositeSource(source)) {
                return false;
            }
        }
        for (AggregationBuilder sub : composite.getSubAggregations()) {
            if (!isPushdownMetric(sub)) {
                return false;
            }
        }
        return true;
    }

    /**
     * A composite source over a plain field with no script, no
     * {@code value_type} hint and {@code missing_bucket} false (a null
     * key opens no bucket, as in the other bucket aggregations):
     * {@code terms} in either order, or {@code date_histogram} with a
     * fixed length interval, {@code offset} 0 and no time zone.
     */
    static boolean isPushdownCompositeSource(CompositeValuesSourceBuilder<?> source) {
        if (source.field() == null
            || source.field().isEmpty()
            || source.script() != null
            || source.userValuetypeHint() != null
            || source.missingBucket()) {
            return false;
        }
        if (source instanceof TermsValuesSourceBuilder) {
            return true;
        }
        if (source instanceof DateHistogramValuesSourceBuilder dateHistogram) {
            return hasFixedLengthInterval(dateHistogram) && dateHistogram.offset() == 0L && dateHistogram.timeZone() == null;
        }
        return false;
    }

    /**
     * Whether the composite date source rounds to a fixed number of
     * milliseconds. The builder exposes its interval only through
     * converters that throw when the configured kind does not convert,
     * so the check is the conversion itself. A {@code fixed_interval}
     * always converts; a {@code calendar_interval} converts when its
     * unit is a day or shorter, and in UTC with no offset, the only
     * configuration accepted here, such a unit rounds exactly like the
     * fixed interval of the same length. Month and larger units, whose
     * length varies, do not convert.
     */
    private static boolean hasFixedLengthInterval(DateHistogramValuesSourceBuilder dateHistogram) {
        try {
            return dateHistogram.getIntervalAsFixed() != null;
        } catch (IllegalStateException | IllegalArgumentException notFixed) {
            return false;
        }
    }

    /**
     * A metric the scan computes, over a field with no script, no
     * {@code missing} substitute and no {@code value_type} hint, and no
     * children of its own: sum / avg / min / max / value_count / stats /
     * extended_stats, cardinality, and tdigest percentiles /
     * percentile_ranks ({@code hdr} keeps its own histogram and stays on
     * the aggregators).
     */
    static boolean isPushdownMetric(AggregationBuilder builder) {
        boolean metric = builder instanceof SumAggregationBuilder
            || builder instanceof AvgAggregationBuilder
            || builder instanceof MinAggregationBuilder
            || builder instanceof MaxAggregationBuilder
            || builder instanceof ValueCountAggregationBuilder
            || builder instanceof StatsAggregationBuilder
            || builder instanceof ExtendedStatsAggregationBuilder
            || builder instanceof CardinalityAggregationBuilder
            || (builder instanceof PercentilesAggregationBuilder percentiles && isTDigest(percentiles.percentilesConfig()))
            || (builder instanceof PercentileRanksAggregationBuilder ranks && isTDigest(ranks.percentilesConfig()));
        if (!metric) {
            return false;
        }
        if (!builder.getSubAggregations().isEmpty() || !builder.getPipelineAggregations().isEmpty()) {
            return false;
        }
        return hasPlainFieldSource((ValuesSourceAggregationBuilder<?>) builder);
    }

    /** The default percentiles method is tdigest, so a request without a {@code tdigest} / {@code hdr} block qualifies. */
    private static boolean isTDigest(PercentilesConfig config) {
        return config == null || config instanceof PercentilesConfig.TDigest;
    }

    /** A terms order on one sub aggregation: its path and direction. */
    record AggregationOrder(String path, boolean ascending) {
    }

    /**
     * The sub aggregation order of a {@code terms}, or null when the
     * order is not one: the order itself, or the compound of the order
     * and the {@code _key} ascending tie breaker the builder wraps
     * every non key order in (any other compound answers null).
     * {@code InternalOrder.Aggregation} exposes its path but not its
     * direction, and a compound does not expose its elements, so both
     * are read back from the order's wire form
     * ({@code InternalOrder.Streams}: compound is -1 followed by the
     * element count, an aggregation order is 0 followed by the
     * direction and the path, {@code _key} ascending is 4).
     */
    static AggregationOrder aggregationOrder(BucketOrder order) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            order.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                byte id = in.readByte();
                if (id == -1) {
                    if (in.readVInt() != 2) {
                        return null;
                    }
                    id = in.readByte();
                    if (id != 0) {
                        return null;
                    }
                    boolean ascending = in.readBoolean();
                    String path = in.readString();
                    return in.readByte() == 4 ? new AggregationOrder(path, ascending) : null;
                }
                if (id == 0) {
                    boolean ascending = in.readBoolean();
                    return new AggregationOrder(in.readString(), ascending);
                }
                return null;
            }
        } catch (IOException impossible) {
            return null;
        }
    }

    /**
     * The DataFusion {@code date_trunc} unit of a {@code date_histogram}
     * {@code calendar_interval}, or {@code null} when the builder has no
     * calendar interval or names a unit {@code date_trunc} does not
     * truncate to. Every calendar unit OpenSearch accepts ({@code second}
     * through {@code year}, in the word or {@code 1x} spelling) has a
     * {@code date_trunc} counterpart with the same rounding for UTC:
     * weeks start on Monday, quarters on January, April, July and
     * October.
     */
    static String calendarUnit(DateHistogramAggregationBuilder dateHistogram) {
        DateHistogramInterval interval = dateHistogram.getCalendarInterval();
        if (interval == null) {
            return null;
        }
        Rounding.DateTimeUnit unit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(interval.toString());
        if (unit == null) {
            return null;
        }
        return switch (unit) {
            case SECOND_OF_MINUTE -> "second";
            case MINUTES_OF_HOUR -> "minute";
            case HOUR_OF_DAY -> "hour";
            case DAY_OF_MONTH -> "day";
            case WEEK_OF_WEEKYEAR -> "week";
            case MONTH_OF_YEAR -> "month";
            case QUARTER_OF_YEAR -> "quarter";
            case YEAR_OF_CENTURY -> "year";
        };
    }

    private static boolean hasPlainFieldSource(ValuesSourceAggregationBuilder<?> builder) {
        return builder.field() != null
            && !builder.field().isEmpty()
            && builder.script() == null
            && builder.missing() == null
            && builder.userValueTypeHint() == null;
    }
}
