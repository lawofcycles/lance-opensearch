/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.execute;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.lance.dispatch.LanceFragmentQueryResponse;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Mutable accumulator that folds every fan-out result into the final
 * response state: the runtime of a
 * {@link org.opensearch.lance.plan.rel.physical.MergeExec}, driven by
 * {@link PlanExecutor}. Per-node aggregation trees reduce through the
 * stock {@link InternalAggregations#topLevelReduce} under the final
 * reduce context carrying the request's pipeline aggregators, per-node hit
 * pages merge by the request's sort ({@link #mergeHits}), and per-node
 * match counts sum under the request's {@code track_total_hits}
 * contract ({@link #totalHits}). Not thread-safe: guarded by the
 * coordinator's sequential index loop.
 */
public final class MergeReducer {

    private static final Logger LOGGER = LogManager.getLogger(MergeReducer.class);

    private final ClusterService clusterService;
    private final BigArrays bigArrays;
    private final ScriptService scriptService;
    private final AggregatorFactories.Builder aggregationsRequested;
    // Sort clauses of the request; drive the cross-node hit merge
    // in buildResponse. Empty means score order.
    private final List<SortBuilder<?>> sorts;
    // Requested pagination window. `perNodeSize` on the wire is
    // `from + size` so every per-node executor already returned
    // enough hits for us to skip the first `from` and keep `size`.
    private final int from;
    private final int size;
    // Whether the request asked for _version / _seq_no /
    // _primary_term envelope fields on hits. Fragment path has
    // no per-doc version accounting (Lance datasets are
    // append/rewrite, not per-doc versioned), so populated
    // values are constant: version=1, seqNo=0, primaryTerm=1.
    // The flags exist so the coordinator only stamps hits when
    // the caller explicitly asked, matching shard path
    // behaviour where these fields default off.
    private final boolean versionRequested;
    private final boolean seqNoAndPrimaryTermRequested;
    // Whether the request asked for track_scores; with a sort that is
    // not score ordered it decides whether max_score is reported, as
    // the shard path's TopDocsCollectorContext decides it.
    private final boolean trackScores;
    // track_total_hits bound the executors counted up to; decides
    // the hits.total relation in buildResponse.
    private final int trackTotalHitsUpTo;
    // The request's collapse field, or null without collapse. The
    // executors already return one hit per distinct value of their
    // fragments; the merge keeps one per value across them.
    private final String collapseField;
    private long totalMatched = 0L;
    // Set when any executor stopped counting at the bound, so the
    // summed total is a lower bound even if it did not exceed
    // trackTotalHitsUpTo itself.
    private boolean matchedIsLowerBound = false;
    // Set when a node of any target did not answer in time: the
    // response is built from the nodes that did, says timed_out,
    // and reports hits.total as a lower bound.
    private boolean timedOut = false;
    // terminated_early of the response: null (left out of the
    // response) unless an executor reported the flag, which only
    // happens when the request carried terminate_after; then true
    // when any executor stopped early, as the shard path reports the
    // flag when any shard did.
    private Boolean terminatedEarly = null;
    // One entry per per-node response, in fan-out order (target
    // order, then node id order within a target). Each inner list
    // is already sorted by the executor and cut to from + size,
    // and carries the target ordinal and row address the merge
    // breaks ties on.
    private final List<List<RankedHit>> perNodeHits = new ArrayList<>();
    // Ordinal of the target whose responses absorbTarget is
    // absorbing; targets arrive one after another in request
    // order, so this is the position of the target in the
    // request's index list.
    private int targetOrdinal = -1;
    private final List<InternalAggregations> perNodeAggregations = new ArrayList<>();

    /** A reducer of a request without {@code collapse}. */
    public MergeReducer(
        ClusterService clusterService,
        BigArrays bigArrays,
        ScriptService scriptService,
        AggregatorFactories.Builder aggregationsRequested,
        List<SortBuilder<?>> sorts,
        int from,
        int size,
        boolean versionRequested,
        boolean seqNoAndPrimaryTermRequested,
        boolean trackScores,
        int trackTotalHitsUpTo
    ) {
        this(
            clusterService,
            bigArrays,
            scriptService,
            aggregationsRequested,
            sorts,
            from,
            size,
            versionRequested,
            seqNoAndPrimaryTermRequested,
            trackScores,
            trackTotalHitsUpTo,
            null
        );
    }

    public MergeReducer(
        ClusterService clusterService,
        BigArrays bigArrays,
        ScriptService scriptService,
        AggregatorFactories.Builder aggregationsRequested,
        List<SortBuilder<?>> sorts,
        int from,
        int size,
        boolean versionRequested,
        boolean seqNoAndPrimaryTermRequested,
        boolean trackScores,
        int trackTotalHitsUpTo,
        String collapseField
    ) {
        this.clusterService = clusterService;
        this.bigArrays = bigArrays;
        this.scriptService = scriptService;
        this.aggregationsRequested = aggregationsRequested;
        this.sorts = sorts;
        this.from = from;
        this.size = size;
        this.versionRequested = versionRequested;
        this.seqNoAndPrimaryTermRequested = seqNoAndPrimaryTermRequested;
        this.trackScores = trackScores;
        this.trackTotalHitsUpTo = trackTotalHitsUpTo;
        this.collapseField = collapseField;
    }

    /** Fold one target's per-node responses (in fan-out order) into the accumulated state. */
    public void absorbTarget(String indexName, List<LanceFragmentQueryResponse> responses, boolean incomplete) {
        timedOut |= incomplete;
        // Every hit needs a SearchShardTarget so the response
        // envelope carries the {@code _index} key that clients
        // expect. Fragment path has no shard concept, so we
        // synthesise one entry keyed on the resolved index
        // metadata; the shard id is always 0 (single-shard).
        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        SearchShardTarget shardTarget = indexMetadata == null
            ? null
            : new SearchShardTarget(
                clusterService.localNode().getId(),
                new ShardId(indexMetadata.getIndex(), 0),
                /* clusterAlias */ null,
                org.opensearch.action.OriginalIndices.NONE
            );
        targetOrdinal++;
        for (LanceFragmentQueryResponse response : responses) {
            totalMatched += response.matched();
            matchedIsLowerBound |= response.matchedIsLowerBound();
            terminatedEarly = mergeTerminatedEarly(terminatedEarly, response.terminatedEarly());
            // Keep each node's list intact; the sort merge and
            // the from/size cut run in buildResponse once every
            // node of every target has answered.
            List<SearchHit> hits = response.hits();
            long[] rowAddrs = response.rowAddrs();
            List<RankedHit> nodeHits = new ArrayList<>(hits.size());
            for (int i = 0; i < hits.size(); i++) {
                SearchHit hit = hits.get(i);
                stampEnvelope(hit, shardTarget);
                nodeHits.add(new RankedHit(hit, targetOrdinal, rowAddrs[i]));
            }
            perNodeHits.add(nodeHits);
            if (response.aggregations() != null) {
                perNodeAggregations.add(response.aggregations());
            }
        }
    }

    /**
     * {@code terminated_early} after one more executor answered: the
     * flag stays absent ({@code null}) while no executor reported one,
     * and is {@code true} once any executor stopped early, the way
     * {@code SearchPhaseController.TopDocsStats} folds the per shard
     * flags.
     */
    public static Boolean mergeTerminatedEarly(Boolean merged, Boolean fromExecutor) {
        if (fromExecutor == null) {
            return merged;
        }
        if (merged == null) {
            return fromExecutor;
        }
        return merged || fromExecutor;
    }

    /**
     * Attach the shard target and, if requested, the constant
     * version / seq_no / primary_term envelope values to a
     * per-node hit. Runs on the coordinator because per-node
     * responses do not know the index name and because the
     * request-level flags live on
     * {@link org.opensearch.search.builder.SearchSourceBuilder}
     * which is not shipped over the wire in full.
     */
    private void stampEnvelope(SearchHit hit, SearchShardTarget shardTarget) {
        if (shardTarget != null) {
            hit.shard(shardTarget);
        }
        if (versionRequested) {
            hit.version(1L);
        }
        if (seqNoAndPrimaryTermRequested) {
            hit.setSeqNo(0L);
            hit.setPrimaryTerm(1L);
        }
    }

    /**
     * {@code hits.total} of the merged response; see
     * {@link #totalHits(long, boolean, int)}. When a node did not
     * answer, the count of the nodes that did is a lower bound of the
     * true count whatever the tracking mode, so the relation is
     * {@code gte}; the value stays as composed (the sum, or the bound
     * when the sum passed it).
     */
    private TotalHits mergedTotalHits() {
        TotalHits total = totalHits(totalMatched, matchedIsLowerBound, trackTotalHitsUpTo);
        if (total == null || !timedOut) {
            return total;
        }
        return new TotalHits(total.value(), TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
    }

    public SearchResponse buildResponse(long startMillis) {
        long took = System.currentTimeMillis() - startMillis;
        // Merge the per-node sorted lists into one ordered list,
        // then apply from/size so the response reflects the
        // requested pagination window. Every node returned up to
        // from + size hits, so the merged list always holds the
        // global top from + size. Under collapse every node returned
        // its top from + size groups, one hit each, and the merged
        // list is cut to one hit per value before the window applies.
        List<SearchHit> hits = mergeHits(perNodeHits, sorts);
        if (collapseField != null) {
            hits = collapseHits(hits, collapseField);
        }
        SearchHit[] paged;
        if (hits.size() <= from) {
            paged = new SearchHit[0];
        } else {
            int end = Math.min(hits.size(), from + size);
            paged = hits.subList(from, end).toArray(new SearchHit[0]);
        }
        // max_score follows the shard path's TopDocsCollectorContext: a
        // score ordered page (no sort, or a leading descending _score
        // clause) reports its top score, a sort with track_scores the
        // largest score of the merged window, and any other sort NaN,
        // although its hits carry a score when a _score clause sits
        // among the sort clauses. The window is read before the from
        // cut, as the shard path reads each shard's top docs before the
        // coordinator skips from (a rescored page reports the best
        // rescored score whatever from is). NaN also when no hit
        // survives the merge.
        float maxScore = Float.NaN;
        if (scoreOrdered(sorts) || trackScores) {
            for (SearchHit hit : hits) {
                float score = hit.getScore();
                if (Float.isNaN(score)) {
                    continue;
                }
                if (Float.isNaN(maxScore) || score > maxScore) {
                    maxScore = score;
                }
            }
        }
        SearchHits searchHits = new SearchHits(paged, mergedTotalHits(), maxScore);
        InternalAggregations aggregations = null;
        if (aggregationsRequested != null && !perNodeAggregations.isEmpty()) {
            // Feed every per-node InternalAggregations tree into the
            // stock reduce path so cross-node reduction lives in
            // OpenSearch's aggregator code rather than in the Lance
            // plugin: nodes ship InternalAggregations, the
            // coordinator calls topLevelReduce. The context is the
            // final one, with the request's pipeline tree, as
            // SearchService.aggReduceContextBuilder builds it for the
            // shard path's SearchPhaseController: topLevelReduce then
            // runs the parent pipelines (cumulative_sum, derivative,
            // bucket_sort, ...) inside each reduced tree and the
            // sibling pipelines (avg_bucket, stats_bucket, ...) over
            // the reduced top level, once, over the merged buckets.
            // The executors never ran them: the aggregators they build
            // from the same factories are the bucket and metric ones,
            // and their slice level reduce is a partial one.
            InternalAggregation.ReduceContext ctx = InternalAggregation.ReduceContext.forFinalReduction(
                bigArrays,
                scriptService,
                /* multiBucketConsumer */ n -> {},
                aggregationsRequested.buildPipelineTree()
            );
            aggregations = InternalAggregations.topLevelReduce(perNodeAggregations, ctx);
        }
        // timed_out is the only trace of a node that did not answer:
        // the fragment path reports one logical unit under _shards,
        // so there is no failed shard to count; the coordinator's
        // WARN log names the node, the fragment count and the
        // timeout. Aggregations are the reduce of the nodes that
        // answered, as on the shard path. terminated_early is left
        // out unless the request carried terminate_after.
        SearchResponseSections sections = new SearchResponseSections(searchHits, aggregations, null, timedOut, terminatedEarly, null, 1);
        // Hide the Lance fragment fan-out from the response
        // shape. The user's mental model is one logical dataset,
        // not N shards; reporting fragmentCount here would leak
        // the Lucene-shard concept back into the API surface
        // that shard-free dispatch is meant to remove. total /
        // successful stay at 1 (single logical unit) so clients
        // scripts that expect at least one successful shard
        // continue to parse cleanly. The physical distribution
        // is still observable through the coordinator's INFO
        // logs and, in the future, dedicated telemetry.
        return new SearchResponse(
            sections,
            null,
            /* totalShards */ 1,
            /* successfulShards */ 1,
            /* skippedShards */ 0,
            took,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    /**
     * Whether the page is in score order: no sort clause, or a leading
     * {@code _score} clause in its default descending direction (the
     * {@code SortField.FIELD_SCORE} the shard path reads the top score
     * from).
     */
    private static boolean scoreOrdered(List<SortBuilder<?>> sorts) {
        if (sorts == null || sorts.isEmpty()) {
            return true;
        }
        return sorts.get(0) instanceof ScoreSortBuilder && sorts.get(0).order() != SortOrder.ASC;
    }

    /**
     * Merge the per-node hit lists into one list ordered the way a
     * single executor would have ordered the union.
     *
     * <p>Each inner list is one node's response, already sorted by
     * that node's executor and cut to {@code from + size}, with the
     * Lance row address of every hit. The merge re-sorts the union
     * with a comparator built from {@code sorts}:
     * <ul>
     *   <li>no sort clause, or a single {@code _score} clause: score
     *       descending ({@link SearchHit#getScore()});</li>
     *   <li>a {@code _score} clause among others: the raw sort value
     *       at that position (the executor stores the score there;
     *       {@link SearchHit#getScore()} is NaN when the request did
     *       not set {@code track_scores}), in the clause's order;</li>
     *   <li>a {@code _doc} clause: row address in the clause's order.
     *       On a single reader over the whole table doc id order is
     *       fragment order then offset, which is row address order,
     *       so this is what the clause means there; the per-node doc
     *       ids the executors report as sort values only order docs
     *       within one node;</li>
     *   <li>any other clause: {@link SearchHit#getRawSortValues()} at
     *       that position, compared as {@link Comparable} in the
     *       clause's order. The executor already substituted the
     *       {@code missing} sentinel for numeric fields, so a null
     *       only arrives for keyword fields; it sorts last unless the
     *       clause says {@code "missing": "_first"}, the same default
     *       OpenSearch's comparator sources apply.</li>
     * </ul>
     * Hits that compare equal are ordered by index (the order of the
     * request's targets) and then by row address ascending. Lucene's
     * collectors break ties by doc id ascending, so every executor
     * returns the tied rows of its fragments in row address order and
     * the lowest addresses of the whole table are always among the
     * per-node pages; sorting the union by the same key therefore
     * yields the page one reader over the whole table would produce,
     * whatever the number of nodes.
     *
     * <p>{@code search_after} needs no handling here: each executor
     * already applied the cursor to its own hits, so every hit in
     * every inner list is past the cursor and the merged order is the
     * correct continuation.
     */
    public static List<SearchHit> mergeHits(List<List<RankedHit>> perNodeHits, List<SortBuilder<?>> sorts) {
        List<RankedHit> ranked = new ArrayList<>();
        for (List<RankedHit> nodeHits : perNodeHits) {
            ranked.addAll(nodeHits);
        }
        if (ranked.size() > 1) {
            ranked.sort(hitComparator(sorts));
        }
        List<SearchHit> out = new ArrayList<>(ranked.size());
        for (RankedHit r : ranked) {
            out.add(r.hit());
        }
        return out;
    }

    /**
     * A per-node hit with what the merge needs to place it: the
     * ordinal of the index it came from in the request's target list
     * and its Lance row address ({@code fragmentId << 32 | offset}).
     * Row addresses are unique within one table, so the pair is a
     * total order over every hit of the request.
     */
    public record RankedHit(SearchHit hit, int target, long rowAddr) {
    }

    /**
     * One hit per distinct value of {@code collapseField} out of the
     * merged, ordered union: the first hit of each value in merged order
     * survives, which is the group's best hit under the request's sort
     * across every executor, and the rest of the group is dropped. This
     * is the walk {@code CollapseTopFieldDocs.merge} does over the
     * shards' collapsed pages on the shard path, with the merged order
     * standing in for its priority queue. The value is read from the
     * doc value field the executor's fetch phase added for the collapse
     * field (the same field the shard path's response carries); a hit
     * without a value belongs to the group of the missing value, as it
     * does in the collapsing collector.
     */
    public static List<SearchHit> collapseHits(List<SearchHit> merged, String collapseField) {
        Set<Object> seen = new HashSet<>();
        List<SearchHit> out = new ArrayList<>(merged.size());
        for (SearchHit hit : merged) {
            if (seen.add(collapseValueOf(hit, collapseField))) {
                out.add(hit);
            }
        }
        return out;
    }

    /** The collapse value of {@code hit}: its doc value field {@code field}'s first value, or null without one. */
    public static Object collapseValueOf(SearchHit hit, String field) {
        DocumentField documentField = hit.field(field);
        return documentField == null ? null : documentField.getValue();
    }

    private static Comparator<RankedHit> hitComparator(List<SortBuilder<?>> sorts) {
        Comparator<RankedHit> tieBreak = Comparator.comparingInt(RankedHit::target).thenComparingLong(RankedHit::rowAddr);
        if (sorts == null || sorts.isEmpty()) {
            return Comparator.<RankedHit>comparingDouble(r -> -scoreOf(r.hit())).thenComparing(tieBreak);
        }
        Comparator<RankedHit> comparator = null;
        for (int i = 0; i < sorts.size(); i++) {
            SortBuilder<?> sort = sorts.get(i);
            Comparator<RankedHit> clause = clauseComparator(sort, i);
            comparator = comparator == null ? clause : comparator.thenComparing(clause);
        }
        return comparator.thenComparing(tieBreak);
    }

    private static Comparator<RankedHit> clauseComparator(SortBuilder<?> sort, int index) {
        boolean descending = sort.order() == SortOrder.DESC;
        if (sort instanceof ScoreSortBuilder) {
            Comparator<RankedHit> byScore = (a, b) -> Float.compare(scoreAt(a.hit(), index), scoreAt(b.hit(), index));
            return descending ? byScore.reversed() : byScore;
        }
        boolean nullsFirst = false;
        if (sort instanceof FieldSortBuilder field) {
            if (FieldSortBuilder.DOC_FIELD_NAME.equals(field.getFieldName())) {
                Comparator<RankedHit> byRowAddr = Comparator.comparingLong(RankedHit::rowAddr);
                return descending ? byRowAddr.reversed() : byRowAddr;
            }
            nullsFirst = "_first".equals(field.missing());
        }
        final boolean nullsFirstFinal = nullsFirst;
        return (a, b) -> {
            Object left = rawSortValue(a.hit(), index);
            Object right = rawSortValue(b.hit(), index);
            if (left == null || right == null) {
                if (left == null && right == null) {
                    return 0;
                }
                // Missing placement is absolute (first or last in
                // the response), not relative to the clause
                // direction, so it is decided before the
                // direction flip below.
                return (left == null) == nullsFirstFinal ? -1 : 1;
            }
            int cmp = compareValues(left, right);
            return descending ? -cmp : cmp;
        };
    }

    private static float scoreOf(SearchHit hit) {
        float score = hit.getScore();
        // NaN would sort above every real score under Float.compare;
        // treat "no score" as the lowest score instead.
        return Float.isNaN(score) ? Float.NEGATIVE_INFINITY : score;
    }

    /**
     * Score for a {@code _score} sort clause: the raw sort value at
     * the clause position when the executor recorded one, else
     * {@link SearchHit#getScore()}.
     */
    private static float scoreAt(SearchHit hit, int index) {
        Object raw = rawSortValue(hit, index);
        if (raw instanceof Number number) {
            return number.floatValue();
        }
        return scoreOf(hit);
    }

    private static Object rawSortValue(SearchHit hit, int index) {
        Object[] raw = hit.getRawSortValues();
        if (raw == null || index >= raw.length) {
            return null;
        }
        return raw[index];
    }

    /**
     * Compare two non-null raw sort values. The executors type a
     * given clause identically on every node (the type comes from
     * the field mapping), so the common case is two values of the
     * same {@link Comparable} class. Mixed numeric widths, which can
     * only happen when two indexes in one request map a field
     * differently, are compared by value. Any other pair has no
     * defined order and would silently scramble the page, so it is
     * refused.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static int compareValues(Object left, Object right) {
        if (left.getClass() == right.getClass() && left instanceof Comparable) {
            return ((Comparable) left).compareTo(right);
        }
        if (left instanceof Number l && right instanceof Number r) {
            if (isIntegral(l) && isIntegral(r)) {
                return Long.compare(l.longValue(), r.longValue());
            }
            return Double.compare(l.doubleValue(), r.doubleValue());
        }
        throw new IllegalStateException(
            "cannot merge sort values of types [" + left.getClass().getName() + "] and [" + right.getClass().getName() + "]"
        );
    }

    private static boolean isIntegral(Number n) {
        return n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte;
    }

    /**
     * {@code hits.total} under the request's {@code track_total_hits}
     * contract, composed the way
     * {@code SearchPhaseController.TopDocsStats#getTotalHits} does it
     * for shard results. {@code null} (no {@code total} block in the
     * response) when tracking is disabled. For {@code track_total_hits:
     * true} the summed per-node count is exact. With an integer bound
     * the relation is {@code gte} when the sum exceeds the bound or any
     * executor stopped counting at it, and the value is then the bound
     * itself, never the sum: each executor counts its own fragments'
     * share of one scan limited to {@code bound + 1} rows, and because
     * Lance picks among tied rows differently on every executor, the
     * shares can add up to less than the bound even though every scan
     * filled. Clients read {@code gte} with the bound as "more than the
     * bound" (the shard path never reports a smaller value with
     * {@code gte}), so the sum is only reported when it is exact.
     *
     * @param totalMatched sum of the per-node matched counts
     * @param matchedIsLowerBound whether any executor stopped counting
     *        at the bound
     * @param trackTotalHitsUpTo the bound the request asked for, or one
     *        of the {@link SearchContext} tracking constants
     */
    public static TotalHits totalHits(long totalMatched, boolean matchedIsLowerBound, int trackTotalHitsUpTo) {
        if (trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
            return null;
        }
        if (trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
            if (matchedIsLowerBound) {
                // An executor may only stop counting under an integer
                // bound; a lower bound with an accurate request is an
                // executor contract bug. The value is still reported as
                // gte so the response does not claim an exactness it
                // does not have.
                LOGGER.warn(
                    "lance.dispatch: executor reported hits.total as a lower bound [{}] although track_total_hits requested "
                        + "an accurate count; reporting gte",
                    totalMatched
                );
                return new TotalHits(totalMatched, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
            }
            return new TotalHits(totalMatched, TotalHits.Relation.EQUAL_TO);
        }
        if (matchedIsLowerBound || totalMatched > trackTotalHitsUpTo) {
            return new TotalHits(trackTotalHitsUpTo, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
        }
        return new TotalHits(totalMatched, TotalHits.Relation.EQUAL_TO);
    }
}
