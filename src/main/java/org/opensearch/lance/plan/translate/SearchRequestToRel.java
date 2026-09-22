/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Collection;

/**
 * Translates one search request body to a logical {@link RelNode} over
 * the index's {@link org.opensearch.lance.plan.rel.LanceTableScan}.
 *
 * <p>The supported shape today is intentionally small: the query is
 * absent or {@code match_all}, {@code size} is 0, and the request
 * carries exactly one top level aggregation which is a metric
 * ({@code sum}, {@code avg}, {@code min}, {@code max},
 * {@code value_count}) over a numeric column. That shape becomes
 * {@code LogicalAggregate(LanceTableScan)}. Every other element throws
 * {@link UnsupportedOperationException} naming the first unsupported
 * element ({@code query type [term]}, {@code size [10] (only 0)},
 * {@code aggregation type [terms]}, {@code two top level aggregations},
 * {@code sort}, {@code post_filter}, {@code _source}, ...); the explain
 * endpoint returns the message in its 400 body, so the message shape is
 * part of the endpoint's contract.
 *
 * <p>{@code timeout}, {@code track_total_hits} and the named writeable
 * envelope flags that only shape a response no plan produces yet are
 * ignored rather than rejected: they select execution behaviour, not
 * plan structure.
 */
public final class SearchRequestToRel {

    private SearchRequestToRel() {}

    /**
     * @param source the parsed search body; null stands for an empty
     *     body and is rejected like a body without aggregations
     * @param model the index's planner model from
     *     {@link LanceSchemas#build} (or a test fixture)
     * @param factory supplies the {@link RelBuilder} the plan is built
     *     with
     * @throws UnsupportedOperationException naming the first unsupported
     *     element of the request
     */
    public static RelNode translate(SearchSourceBuilder source, LanceSchemas.IndexModel model, LancePlannerFactory factory) {
        ValuesSourceAggregationBuilder<?> metric = validate(source);
        RelBuilder relBuilder = factory.relBuilder(model.schema());
        relBuilder.scan(LancePlannerFactory.SCHEMA_NAME, model.indexName());
        AggregationToRel.metric(metric, model.arrowSchema(), model.multiFields(), relBuilder);
        return relBuilder.build();
    }

    /**
     * Checks every element of the body against the supported shape and
     * returns the single metric aggregation. The checks run in a fixed
     * order (query, paging, hit shaping, then aggregations) so the same
     * body always names the same element.
     */
    private static ValuesSourceAggregationBuilder<?> validate(SearchSourceBuilder source) {
        QueryBuilder query = source == null ? null : source.query();
        if (query != null && !(query instanceof MatchAllQueryBuilder)) {
            throw unsupported("query type [" + query.getName() + "]");
        }
        if (source != null && source.from() > 0) {
            throw unsupported("from [" + source.from() + "]");
        }
        int size = source == null || source.size() < 0 ? 10 : source.size();
        if (size != 0) {
            throw unsupported("size [" + size + "] (only 0)");
        }
        if (source.sorts() != null && !source.sorts().isEmpty()) {
            throw unsupported("sort");
        }
        if (source.searchAfter() != null) {
            throw unsupported("search_after");
        }
        if (source.postFilter() != null) {
            throw unsupported("post_filter");
        }
        if (source.fetchSource() != null) {
            throw unsupported("_source");
        }
        if (source.storedFields() != null) {
            throw unsupported("stored_fields");
        }
        if (source.docValueFields() != null && !source.docValueFields().isEmpty()) {
            throw unsupported("docvalue_fields");
        }
        if (source.fetchFields() != null && !source.fetchFields().isEmpty()) {
            throw unsupported("fields");
        }
        if (source.scriptFields() != null && !source.scriptFields().isEmpty()) {
            throw unsupported("script_fields");
        }
        if (source.highlighter() != null) {
            throw unsupported("highlight");
        }
        if (source.suggest() != null) {
            throw unsupported("suggest");
        }
        if (source.collapse() != null) {
            throw unsupported("collapse");
        }
        if (source.rescores() != null && !source.rescores().isEmpty()) {
            throw unsupported("rescore");
        }
        if (source.minScore() != null) {
            throw unsupported("min_score");
        }
        if (source.terminateAfter() > 0) {
            throw unsupported("terminate_after");
        }
        if (source.trackScores()) {
            throw unsupported("track_scores");
        }
        if (Boolean.TRUE.equals(source.explain())) {
            throw unsupported("explain");
        }
        if (Boolean.TRUE.equals(source.version())) {
            throw unsupported("version");
        }
        if (Boolean.TRUE.equals(source.seqNoAndPrimaryTerm())) {
            throw unsupported("seq_no_primary_term");
        }
        if (source.indexBoosts() != null && !source.indexBoosts().isEmpty()) {
            throw unsupported("indices_boost");
        }
        if (source.pointInTimeBuilder() != null) {
            throw unsupported("pit");
        }
        if (source.slice() != null) {
            throw unsupported("slice");
        }
        if (source.profile()) {
            throw unsupported("profile");
        }
        if (source.aggregations() == null || source.aggregations().getAggregatorFactories().isEmpty()) {
            throw unsupported("no aggregations (exactly one metric aggregation)");
        }
        if (!source.aggregations().getPipelineAggregatorFactories().isEmpty()) {
            throw unsupported("pipeline aggregation");
        }
        Collection<AggregationBuilder> aggregations = source.aggregations().getAggregatorFactories();
        if (aggregations.size() > 1) {
            throw unsupported("two top level aggregations");
        }
        AggregationBuilder aggregation = aggregations.iterator().next();
        if (!isSupportedMetric(aggregation)) {
            throw unsupported("aggregation type [" + aggregation.getType() + "]");
        }
        return (ValuesSourceAggregationBuilder<?>) aggregation;
    }

    private static boolean isSupportedMetric(AggregationBuilder aggregation) {
        return aggregation instanceof SumAggregationBuilder
            || aggregation instanceof AvgAggregationBuilder
            || aggregation instanceof MinAggregationBuilder
            || aggregation instanceof MaxAggregationBuilder
            || aggregation instanceof ValueCountAggregationBuilder;
    }

    private static UnsupportedOperationException unsupported(String element) {
        return new UnsupportedOperationException(element);
    }
}
