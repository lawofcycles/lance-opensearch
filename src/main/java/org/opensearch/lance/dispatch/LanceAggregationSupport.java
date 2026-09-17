/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
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
 * <p>Direction 1 Stage 2 adds bucket support (terms / histogram /
 * date_histogram) alongside the metric family shipped in Stage 1.
 * Anything outside the whitelist still returns {@code false} so the
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
        return source != null
            && source.aggregations() != null
            && !source.aggregations().getAggregatorFactories().isEmpty();
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
}
