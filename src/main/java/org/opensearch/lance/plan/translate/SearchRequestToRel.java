/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Translates one search request body to a logical {@link RelNode} over
 * the index's {@link org.opensearch.lance.plan.rel.LanceTableScan}.
 *
 * <p>The supported shape is an aggregation request: {@code size} is 0
 * and the request carries aggregations {@link AggregationToRel}
 * translates (metric only trees, a chain of up to three bucket levels
 * with metric children, {@code composite} over {@code terms} / fixed
 * length {@code date_histogram} sources). The {@code query} clause
 * translates through {@link QueryToRex} and becomes a {@code Filter}
 * directly above the scan (an absent query and {@code match_all} add
 * no filter), so a bucket request plans as the aggregate over its key
 * projection over the filter over the scan. Every other element throws
 * {@link UnsupportedOperationException} naming the first unsupported
 * element ({@code query type [match]}, {@code size [10] (only 0)},
 * {@code aggregation type [top_hits]}, {@code sort},
 * {@code post_filter}, {@code _source}, ...); the explain endpoint
 * returns the message in its 400 body, so the message shape is part of
 * the endpoint's contract.
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
     *     body and is rejected as {@code empty body}
     * @param model the index's planner model from
     *     {@link LanceSchemas#build} (or a test fixture)
     * @param factory supplies the {@link RelBuilder} the plan is built
     *     with
     * @throws UnsupportedOperationException naming the first unsupported
     *     element of the request
     */
    public static RelNode translate(SearchSourceBuilder source, LanceSchemas.IndexModel model, LancePlannerFactory factory) {
        validate(source);
        RelBuilder relBuilder = scanBuilder(model, factory);
        QueryBuilder query = source.query();
        if (query != null && !(query instanceof MatchAllQueryBuilder)) {
            // The filter is created directly rather than through
            // RelBuilder.filter so a constant predicate (match_none)
            // stays a Filter node instead of folding into empty Values.
            // Flattening turns the translator's nested AND / OR chains
            // into the n-ary calls LogicalFilter requires.
            RexNode predicate = RexUtil.flatten(relBuilder.getRexBuilder(), QueryToRex.translate(query, model, relBuilder));
            relBuilder.push(LogicalFilter.create(relBuilder.build(), predicate));
        }
        return AggregationToRel.translate(source.aggregations(), model, relBuilder);
    }

    /**
     * Translates an aggregation tree alone, without the request body
     * envelope checks: the fragment query routing has already gated the
     * envelope (size 0, no post_filter, a scalar or absent query whose
     * filter travels as Lance SQL outside the plan) and holds only the
     * builders. Throws {@link UnsupportedOperationException} naming the
     * first unsupported element, exactly as {@link #translate}.
     */
    public static RelNode translateAggregations(
        AggregatorFactories.Builder aggregations,
        LanceSchemas.IndexModel model,
        LancePlannerFactory factory
    ) {
        if (aggregations == null || aggregations.getAggregatorFactories().isEmpty()) {
            throw unsupported("no aggregations");
        }
        if (!aggregations.getPipelineAggregatorFactories().isEmpty()) {
            throw unsupported("pipeline aggregation");
        }
        return AggregationToRel.translate(aggregations, model, scanBuilder(model, factory));
    }

    /**
     * A builder positioned on the index's scan. Simplification is off
     * so the group key expressions keep the shape the translator spells
     * (a range condition stays {@code >= AND <} instead of folding into
     * a {@code SEARCH} / {@code Sarg}), which the Substrait producer
     * maps term by term.
     */
    private static RelBuilder scanBuilder(LanceSchemas.IndexModel model, LancePlannerFactory factory) {
        RelBuilder relBuilder = factory.relBuilder(model.schema()).transform(config -> config.withSimplify(false));
        relBuilder.scan(LancePlannerFactory.SCHEMA_NAME, model.indexName());
        return relBuilder;
    }

    /**
     * Checks every element of the body against the supported envelope.
     * The checks run in a fixed order (paging, hit shaping, then
     * aggregations) so the same body always names the same element; the
     * query clause reports through its own translation.
     */
    private static void validate(SearchSourceBuilder source) {
        if (source == null) {
            throw unsupported("empty body");
        }
        if (source.from() > 0) {
            throw unsupported("from [" + source.from() + "]");
        }
        int size = source.size() < 0 ? 10 : source.size();
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
            throw unsupported("no aggregations");
        }
        if (!source.aggregations().getPipelineAggregatorFactories().isEmpty()) {
            throw unsupported("pipeline aggregation");
        }
    }

    private static UnsupportedOperationException unsupported(String element) {
        return new UnsupportedOperationException(element);
    }
}
