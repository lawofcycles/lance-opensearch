/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.Collection;

import org.opensearch.common.Rounding;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.Strings;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Compile-time whitelist of aggregation shapes the Lance fragment
 * dispatch path can execute end-to-end. The
 * {@link LanceDispatchActionFilter} consults this when deciding
 * whether to send a search through the fragment coordinator or fall
 * through to shard fan-out.
 *
 * <p>The whitelist covers the metric family plus terms / histogram /
 * date_histogram buckets.
 * Anything outside the whitelist returns {@code false} so the
 * request continues on the shard path — an unknown aggregation type
 * is a compatibility miss, not a request error.
 *
 * <p>The whitelist is intentionally structural. The fragment
 * executor drives OpenSearch's stock aggregator machinery
 * (see {@link TransportLanceFragmentQueryAction}), so anything the
 * standard shard path can run is theoretically reachable — the gate
 * is only about "have we exercised this shape via the fragment
 * dispatch path yet."
 */
final class LanceAggregationSupport {

    private LanceAggregationSupport() {}

    /**
     * @return true if the request either has no aggregations or has
     *     an aggregation tree whose every builder is on the
     *     whitelist. false when at least one aggregation is out of
     *     scope for the fragment path today.
     */
    static boolean isSupported(SearchSourceBuilder source) {
        if (source == null || source.aggregations() == null) {
            return true;
        }
        for (AggregationBuilder top : source.aggregations().getAggregatorFactories()) {
            if (!isBuilderSupported(top)) {
                return false;
            }
        }
        return true;
    }

    /** True when the source carries at least one aggregation. */
    static boolean hasAggregations(SearchSourceBuilder source) {
        return source != null && source.aggregations() != null && !source.aggregations().getAggregatorFactories().isEmpty();
    }

    private static boolean isBuilderSupported(AggregationBuilder builder) {
        if (!(builder instanceof ValuesSourceAggregationBuilder<?> valuesSource)) {
            return false;
        }
        String field = valuesSource.field();
        if (field == null || field.isEmpty()) {
            // Script-based aggregations need a scripting sandbox
            // the fragment executor does not carry.
            return false;
        }
        if (!isRecognisedAggType(builder)) {
            return false;
        }
        // Bucket aggregations may nest metric aggregations. Every
        // sub-aggregation goes through the same whitelist so a
        // date_histogram containing a scripted metric is rejected as
        // a whole.
        for (AggregationBuilder sub : builder.getSubAggregations()) {
            if (!isBuilderSupported(sub)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isRecognisedAggType(AggregationBuilder builder) {
        return builder instanceof SumAggregationBuilder
            || builder instanceof AvgAggregationBuilder
            || builder instanceof MinAggregationBuilder
            || builder instanceof MaxAggregationBuilder
            || builder instanceof ValueCountAggregationBuilder
            || builder instanceof TermsAggregationBuilder
            || builder instanceof HistogramAggregationBuilder
            || builder instanceof DateHistogramAggregationBuilder;
    }

    /**
     * Structural half of the decision to run an aggregation tree as a
     * Substrait group by inside the Lance scan instead of through the
     * Lucene aggregators. Field types are not known here; the executor
     * checks them against the table schema in
     * {@link LanceAggregatePushdown}. The tree qualifies when it is
     * either metric aggregations only, or exactly one bucket
     * aggregation whose children are all metric aggregations, with
     * every builder inside {@link #isPushdownMetric} or
     * {@link #isPushdownBucket}. No pipeline aggregations anywhere.
     */
    static boolean isPushdownCandidate(AggregatorFactories.Builder aggregations) {
        if (aggregations == null || aggregations.getAggregatorFactories().isEmpty()) {
            return false;
        }
        if (!aggregations.getPipelineAggregatorFactories().isEmpty()) {
            return false;
        }
        Collection<AggregationBuilder> top = aggregations.getAggregatorFactories();
        boolean allMetrics = true;
        for (AggregationBuilder builder : top) {
            if (!isPushdownMetric(builder)) {
                allMetrics = false;
                break;
            }
        }
        if (allMetrics) {
            return true;
        }
        if (top.size() != 1) {
            return false;
        }
        AggregationBuilder bucket = top.iterator().next();
        if (!isPushdownBucket(bucket) || !bucket.getPipelineAggregations().isEmpty()) {
            return false;
        }
        for (AggregationBuilder sub : bucket.getSubAggregations()) {
            if (!isPushdownMetric(sub)) {
                return false;
            }
        }
        return true;
    }

    /**
     * A sum / avg / min / max / value_count over a field, with no
     * script, no {@code missing} substitute and no {@code value_type}
     * hint, and no children of its own.
     */
    static boolean isPushdownMetric(AggregationBuilder builder) {
        boolean metric = builder instanceof SumAggregationBuilder
            || builder instanceof AvgAggregationBuilder
            || builder instanceof MinAggregationBuilder
            || builder instanceof MaxAggregationBuilder
            || builder instanceof ValueCountAggregationBuilder;
        if (!metric) {
            return false;
        }
        if (!builder.getSubAggregations().isEmpty() || !builder.getPipelineAggregations().isEmpty()) {
            return false;
        }
        return hasPlainFieldSource((ValuesSourceAggregationBuilder<?>) builder);
    }

    /**
     * The one bucket level the pushdown builds: {@code terms} ordered by
     * {@code _count} descending or by {@code _key} with the default
     * {@code min_doc_count} and no {@code include} / {@code exclude};
     * {@code histogram} with {@code offset} 0 and no bounds;
     * {@code date_histogram} with a {@code fixed_interval} or a
     * {@code calendar_interval} that {@link #calendarUnit} knows,
     * {@code offset} 0, no bounds and no time zone. The remaining options ({@code size},
     * {@code shard_size}, {@code keyed}, {@code min_doc_count} on the
     * histograms, {@code order} on the histograms) are honoured by the
     * result the executor builds or by the coordinator's reduce.
     */
    static boolean isPushdownBucket(AggregationBuilder builder) {
        if (!(builder instanceof ValuesSourceAggregationBuilder<?> valuesSource) || !hasPlainFieldSource(valuesSource)) {
            return false;
        }
        if (builder instanceof TermsAggregationBuilder terms) {
            return (InternalOrder.isCountDesc(terms.order()) || InternalOrder.isKeyOrder(terms.order()))
                && terms.minDocCount() == 1L
                && terms.shardMinDocCount() == 0L
                && terms.includeExclude() == null;
        }
        if (builder instanceof HistogramAggregationBuilder histogram) {
            return histogram.offset() == 0d
                && histogram.interval() > 0d
                && histogram.minBound() == Double.POSITIVE_INFINITY
                && histogram.maxBound() == Double.NEGATIVE_INFINITY
                && !mentionsHardBounds(histogram);
        }
        if (builder instanceof DateHistogramAggregationBuilder dateHistogram) {
            boolean fixed = dateHistogram.getFixedInterval() != null && dateHistogram.getCalendarInterval() == null;
            boolean calendar = dateHistogram.getCalendarInterval() != null
                && dateHistogram.getFixedInterval() == null
                && calendarUnit(dateHistogram) != null;
            return (fixed || calendar)
                && dateHistogram.offset() == 0L
                && dateHistogram.extendedBounds() == null
                && dateHistogram.hardBounds() == null
                && dateHistogram.timeZone() == null;
        }
        return false;
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

    /**
     * {@link HistogramAggregationBuilder} exposes no getter for
     * {@code hard_bounds} (only the setter; {@code extendedBounds()} is
     * protected and the bounds fields are private), so the check goes
     * through the builder's own JSON rendering, which writes the
     * {@code hard_bounds} key only when the option was set. It is the
     * last condition of {@link #isPushdownBucket} and
     * {@link #isPushdownCandidate} runs once per executor request, so
     * the render happens at most once per request and only for a
     * histogram that passed every other condition.
     */
    private static boolean mentionsHardBounds(HistogramAggregationBuilder histogram) {
        return Strings.toString(XContentType.JSON, histogram).contains("\"" + Histogram.HARD_BOUNDS_FIELD.getPreferredName() + "\"");
    }
}
