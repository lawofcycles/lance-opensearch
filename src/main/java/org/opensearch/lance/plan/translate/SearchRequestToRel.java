/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.rel.LanceFtsMatch;
import org.opensearch.lance.plan.rel.LanceKnnSearch;
import org.opensearch.lance.query.LanceFtsBoolQueryBuilder;
import org.opensearch.lance.query.LanceFtsBoostQueryBuilder;
import org.opensearch.lance.query.LanceFtsQueryBuilder;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.lance.query.LanceMatchPhraseQueryBuilder;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.lance.query.LanceMultiMatchQueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Translates one search request body to a logical {@link RelNode} over
 * the index's {@link org.opensearch.lance.plan.rel.LanceTableScan}.
 *
 * <p>The supported shapes are an aggregation request and a full text
 * or knn request. An aggregation request has {@code size} 0 and
 * carries aggregations {@link AggregationToRel} translates (metric
 * only trees, a chain of up to three bucket levels with metric
 * children, {@code composite} over {@code terms} / fixed length
 * {@code date_histogram} sources); its {@code query} clause translates
 * through {@link QueryToRex} and becomes a {@code Filter} directly
 * above the scan (an absent query and {@code match_all} add no
 * filter), so a bucket request plans as the aggregate over its key
 * projection over the filter over the scan. A full text or knn request
 * has a Lance FTS clause ({@code lance_match},
 * {@code lance_match_phrase}, {@code lance_multi_match},
 * {@code lance_fts_bool}, {@code lance_fts_boost}), a {@code bool}
 * whose {@code must} is exactly one such clause with scalar
 * {@code filter} / {@code must_not} companions, or a {@code lance_knn}
 * (optionally with its inner {@code filter}) at the top of the query
 * clause; it plans as {@link LanceFtsMatch} or {@link LanceKnnSearch}
 * over the scalar clauses' {@code Filter} over the scan, carries no
 * aggregations, and its {@code size} shapes the hit page rather than
 * the plan. Every other element throws
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
        LanceShape shape = source == null ? null : detectLanceShape(source.query());
        validate(source, shape != null);
        RelBuilder relBuilder = scanBuilder(model, factory);
        if (shape != null) {
            // A full text or knn request: size shapes the hit page, not
            // the plan (hit projection and top-k are not planned yet),
            // so it is ignored like timeout; aggregations over these
            // shapes are refused by validate.
            return lanceShapeRel(shape, model, relBuilder);
        }
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
     * Translates one {@code query} clause alone, without the request
     * body envelope: the fragment query routing calls this with the
     * rewritten top-level query to obtain the plan the fuse rules run
     * over. A full text shape becomes {@link LanceFtsMatch} and a
     * {@code lance_knn} becomes {@link LanceKnnSearch}, each over the
     * scan or over a {@code Filter} carrying the shape's scalar
     * clauses; any other query becomes the {@code Filter} (or the bare
     * scan for {@code match_all}). Throws
     * {@link UnsupportedOperationException} naming the first
     * unsupported element, exactly as {@link #translate}.
     */
    public static RelNode translateQuery(QueryBuilder query, LanceSchemas.IndexModel model, LancePlannerFactory factory) {
        RelBuilder relBuilder = scanBuilder(model, factory);
        LanceShape shape = detectLanceShape(query);
        if (shape != null) {
            return lanceShapeRel(shape, model, relBuilder);
        }
        if (query != null && !(query instanceof MatchAllQueryBuilder)) {
            RexNode predicate = RexUtil.flatten(relBuilder.getRexBuilder(), QueryToRex.translate(query, model, relBuilder));
            relBuilder.push(LogicalFilter.create(relBuilder.build(), predicate));
        }
        return relBuilder.build();
    }

    /**
     * A top-level query clause the planner represents as its own node:
     * one Lance FTS clause with the scalar clauses of its enclosing
     * {@code bool} (null when the clause stands alone), or a
     * {@code lance_knn} whose inner filter travels as the {@code Filter}
     * input.
     */
    private sealed interface LanceShape permits FtsShape, KnnShape {}

    private record FtsShape(QueryBuilder ftsClause, BoolQueryBuilder scalar) implements LanceShape {
    }

    private record KnnShape(LanceKnnQueryBuilder knn) implements LanceShape {
    }

    /**
     * Detects the supported full text and knn shapes at the top of the
     * query clause: a bare Lance FTS clause, {@code lance_knn}
     * (optionally with its inner {@code filter}), or a {@code bool}
     * whose {@code must} is exactly one FTS clause and whose
     * {@code filter} / {@code must_not} hold no further FTS or knn
     * clause. Returns null for a query without any Lance FTS or knn
     * clause (the scalar path translates it); every other combination
     * (an FTS as a {@code should} clause, an FTS next to other
     * {@code must} clauses, an FTS or knn below {@code filter} /
     * {@code must_not}, a knn inside a {@code bool}) throws
     * {@link UnsupportedOperationException} naming the shape, keeping
     * the contract that those combinations do not push into the Lance
     * scan.
     */
    private static LanceShape detectLanceShape(QueryBuilder query) {
        if (query == null) {
            return null;
        }
        if (query instanceof LanceKnnQueryBuilder knn) {
            return new KnnShape(knn);
        }
        if (query instanceof LanceFtsQueryBuilder) {
            return new FtsShape(query, null);
        }
        if (!(query instanceof BoolQueryBuilder bool)) {
            return null;
        }
        if (!containsLanceClause(bool)) {
            return null;
        }
        for (QueryBuilder clause : bool.should()) {
            if (containsLanceClause(clause)) {
                throw unsupported("full text or knn clause in [should]");
            }
        }
        for (QueryBuilder clause : bool.filter()) {
            if (containsLanceClause(clause)) {
                throw unsupported("full text or knn clause in [filter]");
            }
        }
        for (QueryBuilder clause : bool.mustNot()) {
            if (containsLanceClause(clause)) {
                throw unsupported("full text or knn clause in [must_not]");
            }
        }
        List<QueryBuilder> ftsMust = new ArrayList<>();
        for (QueryBuilder clause : bool.must()) {
            if (clause instanceof LanceKnnQueryBuilder) {
                throw unsupported("knn clause inside [bool]");
            }
            if (clause instanceof LanceFtsQueryBuilder) {
                ftsMust.add(clause);
            } else if (containsLanceClause(clause)) {
                throw unsupported("full text or knn clause below the top level of [must]");
            }
        }
        if (ftsMust.size() > 1) {
            throw unsupported("[must] with more than one full text clause");
        }
        if (ftsMust.size() != 1 || bool.must().size() != 1) {
            throw unsupported("[must] mixing a full text clause with other clauses");
        }
        if (bool.minimumShouldMatch() != null) {
            throw unsupported("minimum_should_match on a bool holding a full text clause");
        }
        if (bool.boost() != 1.0f) {
            throw unsupported("boost on a bool holding a full text clause");
        }
        BoolQueryBuilder scalar = null;
        if (!bool.filter().isEmpty() || !bool.mustNot().isEmpty()) {
            scalar = new BoolQueryBuilder();
            for (QueryBuilder clause : bool.filter()) {
                scalar.filter(clause);
            }
            for (QueryBuilder clause : bool.mustNot()) {
                scalar.mustNot(clause);
            }
        }
        return new FtsShape(ftsMust.get(0), scalar);
    }

    /** Whether {@code query} is, or (through {@code bool} nesting) contains, a Lance FTS or knn clause. */
    private static boolean containsLanceClause(QueryBuilder query) {
        if (query instanceof LanceFtsQueryBuilder || query instanceof LanceKnnQueryBuilder) {
            return true;
        }
        if (query instanceof BoolQueryBuilder bool) {
            for (List<QueryBuilder> clauses : List.of(bool.must(), bool.should(), bool.filter(), bool.mustNot())) {
                for (QueryBuilder clause : clauses) {
                    if (containsLanceClause(clause)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Builds the plan node of a detected shape over the scan the
     * builder holds: the scalar clauses become a {@code Filter} through
     * {@link QueryToRex} (throwing when one of them does not
     * translate), and the FTS or knn node sits above it.
     */
    private static RelNode lanceShapeRel(LanceShape shape, LanceSchemas.IndexModel model, RelBuilder relBuilder) {
        if (shape instanceof FtsShape fts) {
            if (fts.scalar() != null) {
                RexNode predicate = RexUtil.flatten(relBuilder.getRexBuilder(), QueryToRex.translate(fts.scalar(), model, relBuilder));
                relBuilder.push(LogicalFilter.create(relBuilder.build(), predicate));
            }
            RelNode input = relBuilder.build();
            return new LanceFtsMatch(
                input.getCluster(),
                input.getCluster().traitSetOf(Convention.NONE),
                input,
                ftsKindOf(fts.ftsClause()),
                List.copyOf(((LanceFtsQueryBuilder) fts.ftsClause()).referencedFields()),
                fts.ftsClause()
            );
        }
        KnnShape knnShape = (KnnShape) shape;
        LanceKnnQueryBuilder knn = knnShape.knn();
        if (knn.filter() != null) {
            RexNode predicate = RexUtil.flatten(relBuilder.getRexBuilder(), QueryToRex.translate(knn.filter(), model, relBuilder));
            relBuilder.push(LogicalFilter.create(relBuilder.build(), predicate));
        }
        RelNode input = relBuilder.build();
        return new LanceKnnSearch(input.getCluster(), input.getCluster().traitSetOf(Convention.NONE), input, withoutFilter(knn));
    }

    /** The same {@code lance_knn} clause without its inner filter, which the {@code Filter} node carries instead. */
    private static LanceKnnQueryBuilder withoutFilter(LanceKnnQueryBuilder knn) {
        LanceKnnQueryBuilder stripped = new LanceKnnQueryBuilder(knn.field(), knn.vector(), knn.k());
        if (knn.nprobes() != null) {
            stripped.nprobes(knn.nprobes());
        }
        if (knn.refineFactor() != null) {
            stripped.refineFactor(knn.refineFactor());
        }
        if (knn.ef() != null) {
            stripped.ef(knn.ef());
        }
        if (knn.metric() != null) {
            stripped.metric(knn.metric());
        }
        if (knn.useIndex() != null) {
            stripped.useIndex(knn.useIndex());
        }
        stripped.boost(knn.boost());
        if (knn.queryName() != null) {
            stripped.queryName(knn.queryName());
        }
        return stripped;
    }

    private static LanceFtsMatch.Kind ftsKindOf(QueryBuilder ftsClause) {
        if (ftsClause instanceof LanceMatchQueryBuilder) {
            return LanceFtsMatch.Kind.MATCH;
        }
        if (ftsClause instanceof LanceMatchPhraseQueryBuilder) {
            return LanceFtsMatch.Kind.MATCH_PHRASE;
        }
        if (ftsClause instanceof LanceMultiMatchQueryBuilder) {
            return LanceFtsMatch.Kind.MULTI_MATCH;
        }
        if (ftsClause instanceof LanceFtsBoolQueryBuilder) {
            return LanceFtsMatch.Kind.FTS_BOOL;
        }
        if (ftsClause instanceof LanceFtsBoostQueryBuilder) {
            return LanceFtsMatch.Kind.FTS_BOOST;
        }
        throw unsupported("full text clause [" + ftsClause.getWriteableName() + "]");
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
     * query clause reports through its own translation. A body whose
     * query clause is a full text or knn shape ({@code lanceShape})
     * skips the {@code size} check (the page is not part of the plan
     * yet) and refuses aggregations instead of requiring them, because
     * no plan combines an aggregate with a full text or knn node.
     */
    private static void validate(SearchSourceBuilder source, boolean lanceShape) {
        if (source == null) {
            throw unsupported("empty body");
        }
        if (source.from() > 0) {
            throw unsupported("from [" + source.from() + "]");
        }
        int size = source.size() < 0 ? 10 : source.size();
        if (!lanceShape && size != 0) {
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
        if (lanceShape) {
            if (source.aggregations() != null && !source.aggregations().getAggregatorFactories().isEmpty()) {
                throw unsupported("aggregations with a full text or knn query");
            }
            return;
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
