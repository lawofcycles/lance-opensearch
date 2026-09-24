/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelFieldCollation;
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
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.lance.plan.rel.LanceFtsMatch;
import org.opensearch.lance.plan.rel.LanceHitShape;
import org.opensearch.lance.plan.rel.LanceKnnSearch;
import org.opensearch.lance.plan.rel.LanceTopK;
import org.opensearch.lance.plan.rel.physical.FanOutExec;
import org.opensearch.lance.plan.rel.physical.MergeExec;
import org.opensearch.lance.plan.rules.SortResolution;
import org.opensearch.lance.query.LanceFtsBoolQueryBuilder;
import org.opensearch.lance.query.LanceFtsBoostQueryBuilder;
import org.opensearch.lance.query.LanceFtsQueryBuilder;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.lance.query.LanceMatchPhraseQueryBuilder;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.lance.query.LanceMultiMatchQueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Translates one search request body to a logical {@link RelNode} over
 * the index's {@link org.opensearch.lance.plan.rel.LanceTableScan}.
 *
 * <p>Two request families are supported. An aggregation request has
 * {@code size} 0 and carries aggregations {@link AggregationToRel}
 * translates (metric only trees, a chain of up to three bucket levels
 * with metric children, {@code composite} over {@code terms} / fixed
 * length {@code date_histogram} sources); its {@code query} clause
 * translates through {@link QueryToRex} and becomes a {@code Filter}
 * directly above the scan (an absent query and {@code match_all} add
 * no filter), so a bucket request plans as the aggregate over its key
 * projection over the filter over the scan. A hits request has a
 * positive {@code size} and no aggregations; its query root is the
 * scalar {@code Filter} over the scan, or a {@link LanceFtsMatch} /
 * {@link LanceKnnSearch} node for a Lance FTS clause
 * ({@code lance_match}, {@code lance_match_phrase},
 * {@code lance_multi_match}, {@code lance_fts_bool},
 * {@code lance_fts_boost}), a {@code bool} whose {@code must} and
 * {@code should} hold such clauses (fused into one
 * {@code lance_fts_bool} when there are several) with scalar
 * {@code filter} / {@code must_not} companions, or a {@code lance_knn}
 * (optionally with its inner {@code filter}); over the root sit a
 * {@link org.opensearch.lance.plan.rel.LanceTopK} carrying the sort
 * collations, the page size and the {@code search_after} cursor, and a
 * {@link org.opensearch.lance.plan.rel.LanceHitShape} naming the hit
 * envelope. A full text or knn request with {@code size} 0 keeps the
 * bare query tree (it asks for the count, not a page). The query
 * translates under {@link QueryToRex.Scores}: a count, an aggregation
 * request or a page ordered by stored columns reads no score, so a
 * {@code constant_score}, {@code dis_max}, {@code boosting} or
 * {@code function_score} reduces to the rows it matches; a page in
 * score order, a sort naming {@code _score}, {@code track_scores},
 * {@code min_score}, {@code rescore} and {@code collapse} read scores
 * and keep those compounds on the Lucene composition
 * ({@code query type [dis_max] on a scored request}). Every other
 * element throws {@link UnsupportedOperationException} naming the
 * first unsupported element ({@code query type [match]},
 * {@code size [10] (only 0 with aggregations)},
 * {@code aggregation type [multi_terms]}, {@code sort type
 * [_geo_distance]}, {@code post_filter}, {@code _source}, ...); the
 * explain endpoint reports the message of the element that kept a
 * request's envelope on Lucene as {@code unplanned}, so the message
 * shape is part of the endpoint's contract.
 *
 * <p>{@code timeout} and the named writeable
 * envelope flags that only shape a response no plan produces yet are
 * ignored rather than rejected: they select execution behaviour, not
 * plan structure. {@code track_total_hits} selects no structure either,
 * but an explicit bound demands an exact count of the plan, which the
 * coordinator's planner turns into an {@code Accuracy} requirement
 * ({@link ExecutionShape#exactCount()}).
 *
 * <p>Two envelope elements have no plan under any entry: {@code suggest}
 * and {@code highlight}. {@link #checkEnvelopeSupported} refuses a body
 * carrying one before the coordinator translates it, and
 * {@link #unsupportedElement} names the refused element for the explain
 * endpoint; the runtime's 400 and the explain endpoint's
 * {@code unplanned} carry the same message.
 */
public final class SearchRequestToRel {

    private SearchRequestToRel() {}

    /**
     * Translates one search body under the strict envelope
     * ({@link #validate}): the tree of a request whose every element
     * the planner spells, or {@link UnsupportedOperationException}
     * naming the first element it does not. The planner's fixture tests
     * pin plans through this entry; the coordinator and the explain
     * endpoint translate through {@link #translateExecution}, which
     * accepts the runtime envelope.
     *
     * @param source the parsed search body; null stands for an empty
     *     body and is rejected as {@code empty body}
     * @param model the index's planner model from
     *     {@link LanceSchemas#build} (or a test fixture)
     * @param factory supplies the {@link RelBuilder} the plan is built
     *     with
     * @throws UnsupportedOperationException naming the first unsupported
     *     element of the request
     * @throws IllegalArgumentException for an aggregation the fragment
     *     executors cannot run ({@link #checkAggregationsExecutable}),
     *     checked before every other element
     */
    public static RelNode translate(SearchSourceBuilder source, LanceSchemas.IndexModel model, LancePlannerFactory factory) {
        if (source != null) {
            checkAggregationsExecutable(source.aggregations());
        }
        LanceShape shape = source == null ? null : detectLanceShape(source.query());
        validate(source, shape != null);
        RelBuilder relBuilder = scanBuilder(model, factory);
        int size = source.size() < 0 ? 10 : source.size();
        // A size 0 body is a count or an aggregation request and reads no
        // score; a page reads them unless it is ordered by stored columns
        // (validate refused track_scores above).
        QueryToRex.Scores scores = size == 0 ? QueryToRex.Scores.UNUSED : scoresOf(source.sorts());
        RelNode root = queryRoot(source.query(), shape, model, relBuilder, scores);
        if (shape != null) {
            // A size 0 full text or knn request keeps the bare query
            // tree: it asks for the count, not a page.
            return size == 0 ? root : hitsOver(root, source.sorts(), source.searchAfter(), size, null, 0, model);
        }
        if (size == 0) {
            return AggregationToRel.translate(source.aggregations(), model, relBuilder);
        }
        return hitsOver(relBuilder.build(), source.sorts(), source.searchAfter(), size, null, 0, model);
    }

    /**
     * The request elements the coordinator plans a fragment request
     * from: the top level query after the coordinator rewrite, the
     * {@code post_filter}, the sort clauses and cursor, the leading hits
     * skipped ({@code from}), the rows every executor returns
     * ({@code fetch}, the request's {@code from + size}; 0 for a count
     * or aggregation request), the aggregations, and whether the
     * request carries a collector knob ({@code min_score} or
     * {@code terminate_after}). The knobs apply inside Lucene's
     * collectors on the executor, so neither the page nor the aggregate
     * is pushed into the Lance scan for such a request: the query root
     * alone is planned and the collector and the aggregators run over
     * it. {@code secondPass} says whether the request carries a
     * {@code rescore} or a {@code collapse}: both work over the Lucene
     * collector's page (the rescorers need its Lucene scores, the
     * collapse is a collector of its own), so the page is never pushed
     * into the Lance scan for such a request either. Whether an
     * aggregation tree plans into the scan is otherwise not a request
     * element: the translator accepts or refuses the tree's shape and
     * the cost model, fed the routing settings through
     * {@code CostInputs}, chooses between the pushed scan and the Lucene
     * operator. {@code trackTotalHitsUpTo} is the request's
     * {@code track_total_hits} in the encoding
     * {@link SearchSourceBuilder#trackTotalHitsUpTo()} uses, resolved to
     * {@link SearchContext#DEFAULT_TRACK_TOTAL_HITS_UP_TO} when the body
     * leaves the flag out, exactly as the coordinator resolves it; it
     * selects no plan structure but a trait the planner must satisfy
     * ({@link #exactCount()}). {@code trackScores} is the request's
     * {@code track_scores}: a sorted page that reports scores reads them,
     * which decides with the sort and the knobs whether the query's
     * score shaping compounds translate ({@link #scores()}).
     */
    public record ExecutionShape(QueryBuilder query, QueryBuilder postFilter, List<SortBuilder<?>> sorts, Object[] searchAfter, int from,
        int fetch, AggregatorFactories.Builder aggregations, boolean collectorKnobs, boolean secondPass, int trackTotalHitsUpTo,
        boolean trackScores) {

        /** A shape without a {@code rescore} or a {@code collapse}, counting up to the default bound. */
        public ExecutionShape(
            QueryBuilder query,
            QueryBuilder postFilter,
            List<SortBuilder<?>> sorts,
            Object[] searchAfter,
            int from,
            int fetch,
            AggregatorFactories.Builder aggregations,
            boolean collectorKnobs
        ) {
            this(query, postFilter, sorts, searchAfter, from, fetch, aggregations, collectorKnobs, false);
        }

        /** A shape without {@code track_scores}. */
        public ExecutionShape(
            QueryBuilder query,
            QueryBuilder postFilter,
            List<SortBuilder<?>> sorts,
            Object[] searchAfter,
            int from,
            int fetch,
            AggregatorFactories.Builder aggregations,
            boolean collectorKnobs,
            boolean secondPass,
            int trackTotalHitsUpTo
        ) {
            this(query, postFilter, sorts, searchAfter, from, fetch, aggregations, collectorKnobs, secondPass, trackTotalHitsUpTo, false);
        }

        /** A shape counting up to the default {@code track_total_hits} bound. */
        public ExecutionShape(
            QueryBuilder query,
            QueryBuilder postFilter,
            List<SortBuilder<?>> sorts,
            Object[] searchAfter,
            int from,
            int fetch,
            AggregatorFactories.Builder aggregations,
            boolean collectorKnobs,
            boolean secondPass
        ) {
            this(
                query,
                postFilter,
                sorts,
                searchAfter,
                from,
                fetch,
                aggregations,
                collectorKnobs,
                secondPass,
                SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO
            );
        }

        /**
         * The shape the coordinator plans a search body under:
         * {@code size} defaults to 10 and {@code from} to 0 when the body
         * leaves them unset, every executor fetches {@code from + size}
         * rows, and the sort list is empty rather than null.
         *
         * @param source the parsed body; null stands for an empty body
         *     (a {@code match_all} page of 10)
         * @param query the top level query after the coordinator rewrite
         */
        public static ExecutionShape of(SearchSourceBuilder source, QueryBuilder query) {
            int size = source == null || source.size() < 0 ? 10 : source.size();
            int from = source == null || source.from() < 0 ? 0 : source.from();
            List<SortBuilder<?>> sorts = source == null || source.sorts() == null ? List.of() : source.sorts();
            boolean collectorKnobs = source != null && (source.minScore() != null || source.terminateAfter() > 0);
            boolean secondPass = source != null
                && (source.collapse() != null || (source.rescores() != null && !source.rescores().isEmpty()));
            return new ExecutionShape(
                query,
                source == null ? null : source.postFilter(),
                sorts,
                source == null ? null : source.searchAfter(),
                from,
                from + size,
                source == null ? null : source.aggregations(),
                collectorKnobs,
                secondPass,
                source == null || source.trackTotalHitsUpTo() == null
                    ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO
                    : source.trackTotalHitsUpTo(),
                source != null && source.trackScores()
            );
        }

        public boolean hasAggregations() {
            return aggregations != null && !aggregations.getAggregatorFactories().isEmpty();
        }

        public boolean hits() {
            return fetch > 0;
        }

        /** Whether the request carries a {@code search_after} cursor. */
        public boolean hasCursor() {
            return searchAfter != null && searchAfter.length > 0;
        }

        /**
         * Whether the request reads document scores, which decides
         * whether the query's score shaping compounds
         * ({@code constant_score}, {@code dis_max}, {@code boosting},
         * {@code function_score}) translate to the rows they match
         * ({@link QueryToRex.Scores}). Scores are read by a page in
         * score order (no sort, or a sort naming {@code _score}), by
         * {@code track_scores}, by the collector knobs ({@code min_score}
         * filters on the score; {@code terminate_after} rides with it) and
         * by a {@code rescore} or a {@code collapse}. A count, an
         * aggregation request and a page ordered by stored columns read
         * none.
         */
        public QueryToRex.Scores scores() {
            if (collectorKnobs || secondPass || trackScores) {
                return QueryToRex.Scores.USED;
            }
            if (!hits()) {
                return QueryToRex.Scores.UNUSED;
            }
            return scoresOf(sorts);
        }

        /**
         * Whether the request asks for an exact {@code hits.total}: it
         * named {@code track_total_hits} as {@code true} or as a bound
         * other than the default. Such a request demands
         * {@link org.opensearch.lance.plan.traits.Accuracy#EXACT} of the
         * plan. A bound equal to the default, {@code false}, or an absent
         * flag demands nothing: the coordinator resolves an absent flag
         * to the default bound, so the two cannot be told apart here and
         * the default counts as the request's silence.
         */
        public boolean exactCount() {
            return trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_ACCURATE
                || (trackTotalHitsUpTo > 0 && trackTotalHitsUpTo != SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO);
        }
    }

    /**
     * The outcome of {@link #translateExecution}: the logical tree the
     * planner runs over, and the element that kept the request's
     * envelope off the tree when the tree is the query root alone (the
     * translator's message, {@code sort type [_geo_distance]} or
     * {@code aggregation type [multi_terms]}, or the structural reason,
     * {@code aggregations with a post_filter}); null when the envelope
     * translated, or when the request has no envelope to translate (a
     * count).
     */
    public record ExecutionTranslation(RelNode root, String unplanned) {
    }

    /**
     * Translates one request for execution on the fragment path. The
     * tree is the one {@link #translate} builds for the shapes the
     * strict envelope accepts, under the fragment runtime's envelope;
     * the coordinator and the explain endpoint both translate here, so
     * the plan the coordinator ships is the plan explain prints.
     * Elements that shape the fetch phase only
     * ({@code _source}, {@code fields}, {@code track_scores},
     * {@code version}, ...) do not reach this method; {@code from} and
     * {@code post_filter} do and ride on the {@link LanceHitShape}.
     *
     * <p>The query clause must translate: a query outside the
     * translator's vocabulary throws {@link UnsupportedOperationException}
     * naming the element, and the caller decides between a Lucene plan
     * over the request's own builder and a refusal (a filtered knn).
     * An aggregation the fragment executors cannot run at all
     * ({@link #checkAggregationsExecutable}) throws
     * {@link IllegalArgumentException} before anything is translated,
     * which the coordinator and the explain endpoint answer as 400.
     * Every other element is best effort, the way the fragment executor
     * used to decide it per part: an aggregation tree the translator
     * refuses, a sort clause without a collation spelling, a page next
     * to aggregations, aggregations next to a {@code post_filter}, or
     * aggregations over a full text or knn root return the query root
     * alone, and the executor runs that envelope through Lucene over
     * the planned query.
     */
    public static RelNode translateForExecution(ExecutionShape shape, LanceSchemas.IndexModel model, LancePlannerFactory factory) {
        return translateExecution(shape, model, factory).root();
    }

    /**
     * Refuses an aggregation tree holding a builder the fragment
     * executors cannot run ({@code global}, {@code top_hits},
     * {@code rare_terms}, {@code significant_terms},
     * {@code significant_text}) with {@link IllegalArgumentException}
     * naming the builder and the reason. Null or empty passes. The
     * coordinator's planner calls this before it decides anything else
     * about the request, so the refusal does not depend on whether the
     * query translates.
     */
    public static void checkAggregationsExecutable(AggregatorFactories.Builder aggregations) {
        AggregationToRel.checkExecutable(aggregations);
    }

    /**
     * {@link #translateForExecution} together with the element the
     * translator refused, when one kept the envelope off the tree, so
     * the explain endpoint can name why a request runs its envelope
     * through Lucene. The tree is the same one
     * {@link #translateForExecution} returns.
     */
    public static ExecutionTranslation translateExecution(
        ExecutionShape shape,
        LanceSchemas.IndexModel model,
        LancePlannerFactory factory
    ) {
        checkAggregationsExecutable(shape.aggregations());
        LanceShape lanceShape = detectLanceShape(shape.query());
        RelBuilder relBuilder = scanBuilder(model, factory);
        RelNode root = queryRoot(shape.query(), lanceShape, model, relBuilder, shape.scores());
        if (shape.collectorKnobs()) {
            // min_score and terminate_after apply inside Lucene's
            // collectors: the count, the page and the aggregations are
            // whatever those collectors saw, which no Lance side page or
            // aggregate scan can reproduce.
            return new ExecutionTranslation(root, "min_score or terminate_after (applied by the Lucene collectors)");
        }
        boolean hasAggregations = shape.hasAggregations();
        if (!shape.hits()) {
            if (!hasAggregations) {
                return new ExecutionTranslation(root, null);
            }
            if (lanceShape != null) {
                return new ExecutionTranslation(root, "aggregations with a full text or knn query");
            }
            if (shape.postFilter() != null) {
                // A pushed aggregate reports hits.total from its own
                // count, which a post filter would have to narrow; the
                // aggregators and the Lucene count serve that shape.
                return new ExecutionTranslation(root, "aggregations with a post_filter");
            }
            if (!shape.aggregations().getPipelineAggregatorFactories().isEmpty()) {
                return new ExecutionTranslation(root, "pipeline aggregation");
            }
            try {
                return new ExecutionTranslation(AggregationToRel.translate(shape.aggregations(), model, relBuilder), null);
            } catch (UnsupportedOperationException notPlanned) {
                return new ExecutionTranslation(root, notPlanned.getMessage());
            }
        }
        if (hasAggregations) {
            // No plan combines an aggregate with a page: the collector
            // and the aggregators both run over the planned query.
            return new ExecutionTranslation(root, "size [" + shape.fetch() + "] (only 0 with aggregations)");
        }
        if (shape.secondPass()) {
            // A rescore re scores the Lucene scores of the first pass
            // and a collapse is a collector of its own: neither has a
            // page the Lance scan could produce, so the collector runs
            // over the planned query.
            return new ExecutionTranslation(root, "rescore or collapse (a second pass over the Lucene collector's page)");
        }
        try {
            return new ExecutionTranslation(
                hitsOver(root, shape.sorts(), shape.searchAfter(), shape.fetch(), shape.postFilter(), shape.from(), model),
                null
            );
        } catch (UnsupportedOperationException notPlanned) {
            // A sort clause without a collation spelling (geo distance,
            // script, nested, mode, literal missing): the Lucene
            // collector serves the page over the planned query.
            return new ExecutionTranslation(root, notPlanned.getMessage());
        }
    }

    /**
     * The query root over the scan the builder holds, left on the
     * builder as well: the {@link LanceFtsMatch} / {@link LanceKnnSearch}
     * node of a detected shape, the {@code Filter} of a scalar query, or
     * the bare scan for an absent or {@code match_all} query.
     *
     * @param scores whether the request reads document scores, which
     *     decides whether the score shaping compounds translate
     */
    private static RelNode queryRoot(
        QueryBuilder query,
        LanceShape shape,
        LanceSchemas.IndexModel model,
        RelBuilder relBuilder,
        QueryToRex.Scores scores
    ) {
        if (shape != null) {
            RelNode root = lanceShapeRel(shape, model, relBuilder);
            relBuilder.push(root);
            return root;
        }
        if (query != null && !(query instanceof MatchAllQueryBuilder)) {
            // The filter is created directly rather than through
            // RelBuilder.filter so a constant predicate (match_none)
            // stays a Filter node instead of folding into empty Values.
            // Flattening turns the translator's nested AND / OR chains
            // into the n-ary calls LogicalFilter requires.
            RexNode predicate = RexUtil.flatten(relBuilder.getRexBuilder(), QueryToRex.translate(query, model, relBuilder, scores));
            relBuilder.push(LogicalFilter.create(relBuilder.build(), predicate));
        }
        return relBuilder.peek();
    }

    /**
     * Whether a page under {@code sorts} reads document scores: it does
     * without a sort (the page is in score order) and when a clause
     * names {@code _score}; a page ordered by stored columns alone reads
     * none.
     */
    static QueryToRex.Scores scoresOf(List<SortBuilder<?>> sorts) {
        if (sorts == null || sorts.isEmpty()) {
            return QueryToRex.Scores.USED;
        }
        for (SortBuilder<?> sort : sorts) {
            if (sort instanceof ScoreSortBuilder) {
                return QueryToRex.Scores.USED;
            }
        }
        return QueryToRex.Scores.UNUSED;
    }

    /**
     * The coordinator layer over a per-node plan: a {@link MergeExec}
     * whose reduce kind matches what the plan produces, over a
     * {@link FanOutExec} of {@code fanOut} per-node requests cut by
     * {@link FanOutExec.Partitioning#EQUAL_FRAGMENT_GROUPS}, over the
     * plan itself. The per-node tree {@link #translate} builds is
     * untouched; the wrapper only adds the two distributed operators
     * so the fan-out and the reduce are plan nodes the Volcano run
     * lowers (through the coordinator layer converters) and the
     * executor traverses, instead of a hand written path outside the
     * planner's view. An aggregation plan ({@link LanceAggregate}
     * root) reduces through the stock aggregation reduce, a hits plan
     * ({@link LanceHitShape} root) merges the per-node pages, and a
     * count plan (any other root: the bare query tree of a
     * {@code size} 0 full text or knn request, a {@code Filter}, or
     * the scan itself) sums the per-node counts.
     */
    public static RelNode withCoordinatorLayer(RelNode perNodePlan, int fanOut) {
        MergeExec.ReduceKind reduceKind;
        if (perNodePlan instanceof LanceAggregate) {
            reduceKind = MergeExec.ReduceKind.AGGREGATE_INTERNAL;
        } else if (perNodePlan instanceof LanceHitShape) {
            reduceKind = MergeExec.ReduceKind.HITS_TOP_K;
        } else {
            reduceKind = MergeExec.ReduceKind.COUNT_SUM;
        }
        return withCoordinatorLayer(perNodePlan, reduceKind, fanOut);
    }

    /** {@link #withCoordinatorLayer(RelNode, int)} with the reduce kind the caller derived from the request shape. */
    public static RelNode withCoordinatorLayer(RelNode perNodePlan, MergeExec.ReduceKind reduceKind, int fanOut) {
        RelOptCluster cluster = perNodePlan.getCluster();
        FanOutExec fanOutExec = new FanOutExec(
            cluster,
            cluster.traitSetOf(Convention.NONE),
            perNodePlan,
            fanOut,
            FanOutExec.Partitioning.EQUAL_FRAGMENT_GROUPS
        );
        return new MergeExec(cluster, cluster.traitSetOf(Convention.NONE), fanOutExec, reduceKind);
    }

    /**
     * The hits plan of a request asking for a page: the
     * {@link LanceHitShape} envelope over the {@link LanceTopK} that
     * orders and cuts the page over the query root. The sort clauses
     * translate through {@link SortResolution#collationsOf}, throwing
     * for a clause without a collation spelling (a geo distance or
     * script sort, a nested sort, a sort mode, a literal missing
     * value); an absent sort is an empty collation list, the score /
     * row address ordered page. The hit envelope renders {@code _id},
     * {@code _source} over the table columns, sort values when the
     * request sorts, and a numeric score when nothing does (a sorted
     * page reports {@code _score: null} unless {@code track_scores},
     * which the explain envelope refuses). {@code fetch} is the rows
     * every executor returns ({@code from + size} at runtime, the
     * request's {@code size} for explain), {@code postFilter} and
     * {@code from} ride on the hit shape.
     */
    private static RelNode hitsOver(
        RelNode root,
        List<SortBuilder<?>> sorts,
        Object[] searchAfterValues,
        int fetch,
        QueryBuilder postFilter,
        int from,
        LanceSchemas.IndexModel model
    ) {
        List<RelFieldCollation> collations = sorts == null || sorts.isEmpty()
            ? List.of()
            : SortResolution.collationsOf(sorts, root.getRowType(), model);
        List<Object> searchAfter = searchAfterValues == null ? null : Arrays.asList(searchAfterValues);
        RelOptCluster cluster = root.getCluster();
        LanceTopK topK = new LanceTopK(cluster, cluster.traitSetOf(Convention.NONE), root, collations, fetch, 0, searchAfter);
        List<String> outputColumns = new ArrayList<>();
        for (String name : root.getRowType().getFieldNames()) {
            if (!name.startsWith("_")) {
                outputColumns.add(name);
            }
        }
        boolean sorted = !collations.isEmpty();
        boolean scoreOrdered = !sorted;
        for (RelFieldCollation collation : collations) {
            scoreOrdered |= SortResolution.isScoreCollation(root.getRowType(), collation);
        }
        return new LanceHitShape(
            cluster,
            cluster.traitSetOf(Convention.NONE),
            topK,
            outputColumns,
            true,
            true,
            scoreOrdered,
            sorted,
            postFilter,
            from
        );
    }

    /**
     * Translates one {@code query} clause alone, without the request
     * body envelope: the fragment query routing calls this with the
     * rewritten top-level query to obtain the plan the fuse rules run
     * over. A full text shape becomes {@link LanceFtsMatch} and a
     * {@code lance_knn} becomes {@link LanceKnnSearch}, each over the
     * scan or over a {@code Filter} carrying the shape's scalar
     * clauses; any other query becomes the {@code Filter} (or the bare
     * scan for {@code match_all}), with no score read
     * ({@link QueryToRex.Scores#UNUSED}). Throws
     * {@link UnsupportedOperationException} naming the first
     * unsupported element, exactly as {@link #translate}.
     */
    public static RelNode translateQuery(QueryBuilder query, LanceSchemas.IndexModel model, LancePlannerFactory factory) {
        return queryRoot(query, detectLanceShape(query), model, scanBuilder(model, factory), QueryToRex.Scores.UNUSED);
    }

    /**
     * The message the coordinator refuses {@code source} with when the
     * body carries an element no plan of the fragment path answers, or
     * null when every element has a plan. Two elements have none:
     * {@code suggest} and {@code highlight}, which need full text APIs
     * (candidate terms, term positions in the stored text) the Lance
     * Java SDK does not surface. {@code suggest} is checked first, so a
     * body carrying both is refused for its suggester. The search
     * endpoint answers the message as 400
     * ({@link #checkEnvelopeSupported}); the explain endpoint reports
     * the same message under {@code unplanned} with the route
     * {@code unsupported}, so the two agree on which bodies have no
     * plan.
     *
     * @param source the parsed search body; null stands for an empty
     *     body, which every plan answers
     */
    public static String unsupportedElement(SearchSourceBuilder source) {
        if (source == null) {
            return null;
        }
        if (source.suggest() != null) {
            return "search body carries a `suggest` clause which needs full-text APIs Lance does not surface. See `docs/limitations.md`.";
        }
        if (source.highlighter() != null) {
            return "search body carries a `highlight` clause which needs full-text APIs Lance does not surface.";
        }
        return null;
    }

    /**
     * Refuses a search body carrying an element no plan answers with
     * {@link IllegalArgumentException} carrying
     * {@link #unsupportedElement}'s message, which the search endpoint
     * reports as 400. The coordinator calls this before it resolves a
     * target or opens a table, so the refusal costs nothing but the
     * body's parse. A body every plan answers passes.
     */
    public static void checkEnvelopeSupported(SearchSourceBuilder source) {
        String unsupported = unsupportedElement(source);
        if (unsupported != null) {
            throw new IllegalArgumentException(unsupported);
        }
    }

    /**
     * A top-level query clause the planner represents as its own node:
     * one Lance FTS clause (a single DSL clause, or the
     * {@code lance_fts_bool} the FTS clauses of a stock {@code bool}
     * fuse into) with the scalar clauses of its enclosing {@code bool}
     * (null when the clause stands alone), or a {@code lance_knn} whose
     * inner filter travels as the {@code Filter} input.
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
     * holding Lance FTS clauses. Returns null for a query without any
     * Lance FTS or knn clause (the scalar path translates it).
     *
     * <p>A {@code bool} is accepted when its {@code must} and
     * {@code should} lists hold FTS clauses only, its {@code filter}
     * holds scalar clauses only, and its {@code must_not} holds any mix
     * of the two; it needs at least one {@code must} or {@code should}
     * clause, no {@code minimum_should_match} and a boost of 1. The FTS
     * clauses fuse into one {@link LanceFtsBoolQueryBuilder} with the
     * same occurrences, which Lance scores as Lucene's
     * {@code BooleanQuery} would: required clauses intersect and add
     * their scores, optional clauses add theirs when they match,
     * prohibited clauses exclude. Two Lucene rules bound the fuse. A
     * {@code should} next to a {@code filter} without a {@code must} is
     * optional in Lucene (the filter alone selects the rows) but
     * required in Lance's boolean, so that shape is refused. A clause
     * boost travels inside the Lance tree only on a match or multi
     * match ({@link LanceFtsQueryBuilder#boostRepresentable}), so a
     * boosted phrase or nested boolean refuses the fuse. A single FTS
     * clause in {@code must}, or alone in {@code should} without a
     * filter, stays that clause. The scalar {@code filter} and
     * {@code must_not} clauses become the shape's filter. Every other
     * combination (a scalar clause in {@code must} or {@code should}, an
     * FTS clause below {@code filter}, a knn inside a {@code bool}, an
     * FTS clause nested below the top level of a list) throws
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
        for (QueryBuilder clause : bool.filter()) {
            if (containsLanceClause(clause)) {
                throw unsupported("full text or knn clause in [filter]");
            }
        }
        List<QueryBuilder> ftsMust = ftsClausesOf(bool.must(), "must");
        List<QueryBuilder> ftsShould = ftsClausesOf(bool.should(), "should");
        List<QueryBuilder> ftsMustNot = new ArrayList<>();
        List<QueryBuilder> scalarMustNot = new ArrayList<>();
        for (QueryBuilder clause : bool.mustNot()) {
            if (clause instanceof LanceKnnQueryBuilder) {
                throw unsupported("knn clause inside [bool]");
            }
            if (clause instanceof LanceFtsQueryBuilder) {
                ftsMustNot.add(clause);
            } else if (containsLanceClause(clause)) {
                throw unsupported("full text or knn clause below the top level of [must_not]");
            } else {
                scalarMustNot.add(clause);
            }
        }
        if (ftsMust.isEmpty() && ftsShould.isEmpty()) {
            throw unsupported("full text clause in [must_not] without a [must] or [should] full text clause");
        }
        if (ftsMust.isEmpty() && !bool.filter().isEmpty()) {
            // Lucene requires one should clause only when no must or
            // filter clause is present; the filter would make the FTS
            // clauses optional, which Lance's boolean cannot express.
            throw unsupported("full text clause in [should] next to [filter] without a [must] clause");
        }
        if (bool.minimumShouldMatch() != null) {
            throw unsupported("minimum_should_match on a bool holding a full text clause");
        }
        if (bool.boost() != 1.0f) {
            throw unsupported("boost on a bool holding a full text clause");
        }
        QueryBuilder ftsClause;
        if (ftsMust.size() == 1 && ftsShould.isEmpty() && ftsMustNot.isEmpty()) {
            ftsClause = ftsMust.get(0);
        } else if (ftsMust.isEmpty() && ftsShould.size() == 1 && ftsMustNot.isEmpty()) {
            ftsClause = ftsShould.get(0);
        } else {
            ftsClause = fuse(ftsMust, ftsShould, ftsMustNot);
        }
        BoolQueryBuilder scalar = null;
        if (!bool.filter().isEmpty() || !scalarMustNot.isEmpty()) {
            scalar = new BoolQueryBuilder();
            for (QueryBuilder clause : bool.filter()) {
                scalar.filter(clause);
            }
            for (QueryBuilder clause : scalarMustNot) {
                scalar.mustNot(clause);
            }
        }
        return new FtsShape(ftsClause, scalar);
    }

    /**
     * The clauses of a {@code must} or {@code should} list, which must
     * all be Lance FTS clauses: a knn, a nested FTS or a scalar clause
     * next to an FTS clause refuses the shape.
     */
    private static List<QueryBuilder> ftsClausesOf(List<QueryBuilder> clauses, String occur) {
        List<QueryBuilder> fts = new ArrayList<>();
        List<QueryBuilder> other = new ArrayList<>();
        for (QueryBuilder clause : clauses) {
            if (clause instanceof LanceKnnQueryBuilder) {
                throw unsupported("knn clause inside [bool]");
            }
            if (clause instanceof LanceFtsQueryBuilder) {
                fts.add(clause);
            } else if (containsLanceClause(clause)) {
                throw unsupported("full text or knn clause below the top level of [" + occur + "]");
            } else {
                other.add(clause);
            }
        }
        if (!fts.isEmpty() && !other.isEmpty()) {
            throw unsupported("[" + occur + "] mixing a full text clause with other clauses");
        }
        return fts;
    }

    /**
     * One {@code lance_fts_bool} holding the FTS clauses of a stock
     * {@code bool} under their occurrences. A clause whose boost Lance
     * cannot carry refuses the fuse.
     */
    private static LanceFtsBoolQueryBuilder fuse(List<QueryBuilder> must, List<QueryBuilder> should, List<QueryBuilder> mustNot) {
        LanceFtsBoolQueryBuilder fused = new LanceFtsBoolQueryBuilder();
        for (QueryBuilder clause : must) {
            fused.must(fusable(clause));
        }
        for (QueryBuilder clause : should) {
            fused.should(fusable(clause));
        }
        for (QueryBuilder clause : mustNot) {
            fused.mustNot(fusable(clause));
        }
        return fused;
    }

    private static QueryBuilder fusable(QueryBuilder clause) {
        if (!LanceFtsQueryBuilder.boostRepresentable(clause)) {
            throw unsupported("boost on a [" + clause.getWriteableName() + "] clause inside a bool of several full text clauses");
        }
        return clause;
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
     * query clause reports through its own translation. A body with
     * {@code size} 0 is an aggregation request (or, for a full text /
     * knn shape, a count) and refuses sort and cursor elements; a body
     * with a positive {@code size} is a hits request, refuses
     * aggregations (no plan combines an aggregate with a page), and
     * accepts {@code sort} and {@code search_after} (the cursor only
     * together with a sort, matching the executor's dispatch gate).
     */
    private static void validate(SearchSourceBuilder source, boolean lanceShape) {
        if (source == null) {
            throw unsupported("empty body");
        }
        if (source.from() > 0) {
            throw unsupported("from [" + source.from() + "]");
        }
        int size = source.size() < 0 ? 10 : source.size();
        boolean hits = size != 0;
        boolean hasAggregations = source.aggregations() != null && !source.aggregations().getAggregatorFactories().isEmpty();
        if (lanceShape && hasAggregations) {
            throw unsupported("aggregations with a full text or knn query");
        }
        if (hits && hasAggregations) {
            throw unsupported("size [" + size + "] (only 0 with aggregations)");
        }
        if (!hits && source.sorts() != null && !source.sorts().isEmpty()) {
            throw unsupported("sort");
        }
        if (source.searchAfter() != null) {
            if (!hits) {
                throw unsupported("search_after");
            }
            if (source.sorts() == null || source.sorts().isEmpty()) {
                throw unsupported("search_after without sort");
            }
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
        if (lanceShape || hits) {
            // Aggregations were refused above; a hits request has no
            // further envelope to check.
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
