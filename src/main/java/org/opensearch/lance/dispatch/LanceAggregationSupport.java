/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.Collection;

import org.opensearch.common.Rounding;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.Strings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentileRanksAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.opensearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Compile-time allow list of aggregation shapes the Lance fragment
 * dispatch path can execute end-to-end. The
 * {@link LanceDispatchActionFilter} consults this when deciding
 * whether to send a search through the fragment coordinator or fall
 * through to shard fan-out.
 *
 * <p>The list covers the metric family (sum, avg, min, max,
 * value_count, stats, extended_stats, percentiles, percentile_ranks,
 * cardinality), the field bucket aggregations (terms, histogram,
 * date_histogram, range, date_range, missing, composite over terms /
 * histogram / date_histogram sources) and the query bucket
 * aggregations (filter, filters) whose queries are scalar filters.
 * Anything outside the list returns {@code false} so the request
 * continues on the shard path: an unknown aggregation type is a
 * compatibility miss, not a request error.
 *
 * <p>The list is intentionally structural. The fragment executor drives
 * OpenSearch's stock aggregator machinery (see
 * {@link TransportLanceFragmentQueryAction}) and the coordinator reduces
 * the per node {@code InternalAggregation} trees with the stock
 * {@code InternalAggregations.topLevelReduce}, so every aggregation
 * whose result is a stock {@code InternalAggregation} is reachable; the
 * gate is only about "has this shape been exercised on the fragment
 * path". The shapes that stay off the list need something the fragment
 * executor does not run: scripts (no script sandbox is set up per
 * fragment request), {@code top_hits} / {@code sampler} /
 * {@code significant_terms} (a fetch phase or a background frequency
 * set), {@code nested} / {@code reverse_nested} / {@code geo*} (field
 * types the Lance mapping does not derive), {@code scripted_metric},
 * and every pipeline aggregation: the coordinator reduces with
 * {@code PipelineTree.EMPTY}, so a pipeline would never run.
 *
 * <p>Approximate aggregations ({@code percentiles} with the default
 * tdigest, {@code cardinality}) merge one sketch per executor on the
 * fragment path where the single shard of the shard path keeps one
 * sketch over the whole table, so their values can differ from the shard
 * path within the algorithm's error, as they would between shards of an
 * ordinary index.
 */
final class LanceAggregationSupport {

    private LanceAggregationSupport() {}

    /**
     * Deepest bucket level the pushdown builds: {@code terms > terms >
     * terms}. Every level multiplies the number of key combinations,
     * and the scan returns one row per combination.
     */
    static final int MAX_PUSHDOWN_BUCKET_DEPTH = 3;

    /**
     * @return true if the request either has no aggregations or has
     *     an aggregation tree whose every builder is on the
     *     allow list. false when at least one aggregation is out of
     *     scope for the fragment path today.
     */
    static boolean isSupported(SearchSourceBuilder source) {
        if (source == null || source.aggregations() == null) {
            return true;
        }
        if (!source.aggregations().getPipelineAggregatorFactories().isEmpty()) {
            return false;
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

    /**
     * Whether {@code builder} and every aggregation below it are on the
     * allow list. A pipeline aggregation anywhere in the subtree rejects
     * the whole tree (see the class comment); so does a sub aggregation
     * off the list, so a {@code date_histogram} containing a scripted
     * metric is rejected as a whole.
     */
    static boolean isBuilderSupported(AggregationBuilder builder) {
        if (!builder.getPipelineAggregations().isEmpty()) {
            return false;
        }
        if (!isRecognisedAggType(builder)) {
            return false;
        }
        for (AggregationBuilder sub : builder.getSubAggregations()) {
            if (!isBuilderSupported(sub)) {
                return false;
            }
        }
        return true;
    }

    /**
     * The allow list proper, one builder at a time without its children.
     * A field aggregation ({@link ValuesSourceAggregationBuilder}) has to
     * name a field and carry no script; the remaining options
     * ({@code missing}, {@code value_type}, {@code order}, {@code size},
     * percentile method, precision threshold, ranges, keyed) are honoured
     * by the stock aggregator the executor runs. A {@code composite}
     * qualifies when every source is a terms / histogram /
     * date_histogram source over a field without a script. A
     * {@code filter} or {@code filters} qualifies when every query is a
     * scalar filter ({@link #isFilterQuerySupported}).
     */
    static boolean isRecognisedAggType(AggregationBuilder builder) {
        if (builder instanceof ValuesSourceAggregationBuilder<?> valuesSource) {
            return namesFieldWithoutScript(valuesSource.field(), valuesSource.script() != null) && isRecognisedFieldAggregation(builder);
        }
        if (builder instanceof CompositeAggregationBuilder composite) {
            if (composite.sources().isEmpty()) {
                return false;
            }
            for (CompositeValuesSourceBuilder<?> source : composite.sources()) {
                boolean knownSource = source instanceof TermsValuesSourceBuilder
                    || source instanceof HistogramValuesSourceBuilder
                    || source instanceof DateHistogramValuesSourceBuilder;
                if (!knownSource || !namesFieldWithoutScript(source.field(), source.script() != null)) {
                    return false;
                }
            }
            return true;
        }
        if (builder instanceof FilterAggregationBuilder filter) {
            return isFilterQuerySupported(filter.getFilter());
        }
        if (builder instanceof FiltersAggregationBuilder filters) {
            if (filters.filters().isEmpty()) {
                return false;
            }
            for (FiltersAggregator.KeyedFilter keyed : filters.filters()) {
                if (!isFilterQuerySupported(keyed.filter())) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private static boolean namesFieldWithoutScript(String field, boolean hasScript) {
        // Script-based aggregations need a scripting sandbox the
        // fragment executor does not carry.
        return field != null && !field.isEmpty() && !hasScript;
    }

    private static boolean isRecognisedFieldAggregation(AggregationBuilder builder) {
        return builder instanceof SumAggregationBuilder
            || builder instanceof AvgAggregationBuilder
            || builder instanceof MinAggregationBuilder
            || builder instanceof MaxAggregationBuilder
            || builder instanceof ValueCountAggregationBuilder
            || builder instanceof StatsAggregationBuilder
            || builder instanceof ExtendedStatsAggregationBuilder
            || builder instanceof PercentilesAggregationBuilder
            || builder instanceof PercentileRanksAggregationBuilder
            || builder instanceof CardinalityAggregationBuilder
            || builder instanceof TermsAggregationBuilder
            || builder instanceof HistogramAggregationBuilder
            || builder instanceof DateHistogramAggregationBuilder
            || builder instanceof RangeAggregationBuilder
            || builder instanceof DateRangeAggregationBuilder
            || builder instanceof MissingAggregationBuilder;
    }

    /**
     * Whether the query of a {@code filter} / {@code filters} bucket can
     * run inside the fragment executor's aggregation pass: a
     * {@code match_all}, {@code term}, {@code terms}, {@code range} or
     * {@code exists} query, or a {@code bool} whose every clause is one
     * of those. These resolve to Lucene queries over the leaf readers'
     * doc values, the same way the top level scalar filters do. A
     * {@code lance_match}, {@code lance_knn}, or a {@code match} on a
     * {@code lance_text} field would resolve to a Lance query whose
     * Weight runs its own Lance scan per aggregation bucket, outside the
     * single scan the executor plans for the request, so those stay on
     * the shard path; so does every other query type until it has been
     * exercised here. A {@code null} query (a {@code filter} bucket
     * without a query) is a {@code match_all} to the aggregator.
     */
    static boolean isFilterQuerySupported(QueryBuilder query) {
        if (query == null
            || query instanceof MatchAllQueryBuilder
            || query instanceof TermQueryBuilder
            || query instanceof TermsQueryBuilder
            || query instanceof RangeQueryBuilder
            || query instanceof ExistsQueryBuilder) {
            return true;
        }
        if (query instanceof BoolQueryBuilder bool) {
            for (QueryBuilder clause : bool.must()) {
                if (!isFilterQuerySupported(clause)) {
                    return false;
                }
            }
            for (QueryBuilder clause : bool.filter()) {
                if (!isFilterQuerySupported(clause)) {
                    return false;
                }
            }
            for (QueryBuilder clause : bool.should()) {
                if (!isFilterQuerySupported(clause)) {
                    return false;
                }
            }
            for (QueryBuilder clause : bool.mustNot()) {
                if (!isFilterQuerySupported(clause)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Structural half of the decision to run an aggregation tree as a
     * Substrait group by inside the Lance scan instead of through the
     * Lucene aggregators. Field types are not known here; the executor
     * checks them against the table schema in
     * {@link LanceAggregatePushdown}. The tree qualifies when it is one
     * of the following, with no pipeline aggregations anywhere:
     * <ul>
     *   <li>metric aggregations only ({@link #isPushdownMetric});</li>
     *   <li>one bucket aggregation ({@link #isPushdownBucket}) whose
     *       children are metrics plus at most one further bucket
     *       aggregation of the same kind, down to
     *       {@link #MAX_PUSHDOWN_BUCKET_DEPTH} levels. Two bucket
     *       aggregations side by side would need two group bys;</li>
     *   <li>one {@code composite} ({@link #isPushdownComposite}) whose
     *       children are all metrics.</li>
     * </ul>
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
        AggregationBuilder root = top.iterator().next();
        if (root instanceof CompositeAggregationBuilder composite) {
            return isPushdownComposite(composite);
        }
        return isPushdownBucketTree(root, 1);
    }

    private static boolean isPushdownBucketTree(AggregationBuilder bucket, int depth) {
        if (depth > MAX_PUSHDOWN_BUCKET_DEPTH || !isPushdownBucket(bucket) || !bucket.getPipelineAggregations().isEmpty()) {
            return false;
        }
        boolean nestedBucketSeen = false;
        for (AggregationBuilder sub : bucket.getSubAggregations()) {
            if (isPushdownMetric(sub)) {
                continue;
            }
            if (nestedBucketSeen || !isPushdownBucketTree(sub, depth + 1)) {
                return false;
            }
            nestedBucketSeen = true;
        }
        return true;
    }

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
     * One bucket level the pushdown builds: {@code terms} ordered by
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
