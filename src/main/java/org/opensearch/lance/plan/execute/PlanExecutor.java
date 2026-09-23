/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.execute;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.calcite.rel.RelNode;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.dispatch.LanceFragmentQueryRequest;
import org.opensearch.lance.engine.LanceCancellation;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.rel.physical.FanOutExec;
import org.opensearch.lance.plan.rel.physical.MergeExec;
import org.opensearch.lance.plan.rel.physical.ShardPathFallbackExec;
import org.opensearch.lance.query.LanceFtsQuery;
import org.opensearch.search.approximate.ApproximateScoreQuery;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.tasks.CancellableTask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Executes the planner's physical operators on the coordinator side:
 * it traverses a {@code MergeExec (FanOutExec (per node plan))} tree
 * ({@link #execute}): the {@link FanOutExec} runs as one
 * {@link FragmentFanOut} of per-node transport requests over the
 * fragment groups its partitioning cut, and the {@link MergeExec}
 * runs as the {@link MergeReducer} absorption of the gathered
 * responses. The per node plan below the fan-out travels to the
 * fragment executors as a {@link FragmentPlan}; what they need beyond
 * that plan is the matched-count route of the scan
 * ({@link #computeMatched} with the count-only scans below it), which
 * this class owns as well. The Lucene collector and aggregator bodies
 * stay with the transport actions.
 */
public final class PlanExecutor {

    private final LancePlannerFactory plannerFactory;

    public PlanExecutor(LancePlannerFactory plannerFactory) {
        this.plannerFactory = plannerFactory;
    }

    /** The factory this executor plans with, for callers that derive plans of their own. */
    public LancePlannerFactory plannerFactory() {
        return plannerFactory;
    }

    // ------------------------------------------------------------------
    // Coordinator side: MergeExec over FanOutExec.
    // ------------------------------------------------------------------

    /**
     * Everything one fan-out needs beyond the plan itself: the
     * fragment groups the partitioning cut, the transport payload of
     * each ({@code requests}), how a request leaves the node
     * ({@code sender}), the pool the merge runs on, the pool and the
     * listener a node that will not answer is reported through, and
     * the partial results / cancellation policy.
     */
    public record FanOutContext(List<FragmentGroup> groups, Function<FragmentGroup, LanceFragmentQueryRequest> requests,
        FragmentFanOut.Sender sender, Executor mergeExecutor, Executor notifyExecutor,
        FragmentFanOut.IncompleteNodeListener incompleteListener, boolean allowPartialResults, CancellableTask task) {
    }

    /**
     * Traverse one coordinator plan and run it: the root must be the
     * {@link MergeExec} over the {@link FanOutExec} the translator
     * wrapper builds. The fan-out sends one request per group of
     * {@code ctx} in slot order; once every node has answered, the
     * merge absorbs the responses into {@code reducer} under
     * {@code indexName} and {@code done} completes. Failure, timeout
     * and cancellation behaviour is {@link FragmentFanOut}'s.
     */
    public void execute(RelNode coordinatorPlan, FanOutContext ctx, MergeReducer reducer, String indexName, ActionListener<Void> done) {
        if (!(coordinatorPlan instanceof MergeExec merge)) {
            throw new IllegalArgumentException("the coordinator plan root must be a MergeExec, got " + coordinatorPlan);
        }
        if (!(merge.getInput() instanceof FanOutExec fanOut)) {
            throw new IllegalArgumentException("the merge input must be a FanOutExec, got " + merge.getInput());
        }
        executeFanOut(fanOut, ctx, outcome -> executeMerge(merge, reducer, indexName, outcome), done);
    }

    /** One {@link FragmentFanOut} of per-node requests, in the slot order of the groups. */
    private static void executeFanOut(
        FanOutExec fanOut,
        FanOutContext ctx,
        Consumer<FragmentFanOut.Outcome> merge,
        ActionListener<Void> done
    ) {
        FragmentFanOut fan = new FragmentFanOut(
            ctx.groups().size(),
            ctx.mergeExecutor(),
            merge,
            done,
            ctx.allowPartialResults(),
            ctx.task(),
            ctx.notifyExecutor(),
            ctx.incompleteListener()
        );
        int slot = 0;
        for (FragmentGroup group : ctx.groups()) {
            fan.send(ctx.sender(), slot++, group.node(), ctx.requests().apply(group));
        }
    }

    /** The reduce of one target's gathered responses into the accumulated state. */
    private static void executeMerge(MergeExec merge, MergeReducer reducer, String indexName, FragmentFanOut.Outcome outcome) {
        reducer.absorbTarget(indexName, outcome.responses(), outcome.incompleteNodes() > 0);
    }

    /**
     * Run one shard path fallback plan: the root must be the
     * {@link ShardPathFallbackExec} the shard path rule produced. The
     * operator's execution is the forward itself — the whole
     * {@code SearchRequest} leaves the planner's tree and continues on
     * OpenSearch's standard shard search path, per request, exactly as
     * if the dispatch filter had dropped it back onto the filter chain
     * — so {@code forward} carries the caller's chain continuation
     * (including its reader-bound guard and its thread pool hop) and
     * nothing below the root is traversed.
     */
    public void executeShardPath(RelNode shardPathPlan, Runnable forward) {
        if (!(shardPathPlan instanceof ShardPathFallbackExec)) {
            throw new IllegalArgumentException("the shard path plan root must be a ShardPathFallbackExec, got " + shardPathPlan);
        }
        forward.run();
    }

    /**
     * The fragments of one per-node request: a node's fragments, or the
     * {@code groupIndex}th of {@code groupCount} contiguous groups of them
     * when they do not fit one Lucene reader together, with the physical
     * rows of the group.
     */
    public record FragmentGroup(DiscoveryNode node, List<Integer> fragmentIds, long rows, int groupIndex, int groupCount) {
    }

    /**
     * The {@link FanOutExec.Partitioning#EQUAL_FRAGMENT_GROUPS} cut:
     * fragments dealt round-robin across the sorted data-node list
     * ({@link #groupFragmentsByNode}), each node's share split into
     * groups whose rows fit one Lucene reader ({@link #splitByRows}).
     */
    public static List<FragmentGroup> equalFragmentGroups(
        List<Integer> fragmentIds,
        List<Long> fragmentRows,
        List<DiscoveryNode> nodeList,
        long maxDocs
    ) {
        return splitByRows(groupFragmentsByNode(fragmentIds, nodeList), fragmentIds, fragmentRows, maxDocs);
    }

    /**
     * Round-robin fragment ids across the sorted data-node list.
     * Empty per-node bucket entries are omitted so downstream
     * dispatch code only sees nodes that actually own work. The map
     * iterates in {@code nodeList} order so the fan-out is
     * deterministic.
     */
    public static Map<DiscoveryNode, List<Integer>> groupFragmentsByNode(List<Integer> fragmentIds, List<DiscoveryNode> nodeList) {
        Map<DiscoveryNode, List<Integer>> result = new LinkedHashMap<>();
        for (int i = 0; i < fragmentIds.size(); i++) {
            DiscoveryNode node = nodeList.get(i % nodeList.size());
            result.computeIfAbsent(node, k -> new ArrayList<>()).add(fragmentIds.get(i));
        }
        return result;
    }

    /**
     * Cut every node's fragment list of {@code perNode} into contiguous
     * groups whose physical rows fit in {@code maxDocs}, in the order of
     * the map and of each list ({@link LanceDirectoryReader#groupEnds}).
     * {@code fragmentIds} and {@code fragmentRows} are the table's
     * fragments and their physical rows in the same order; a fragment id
     * the rows are not known for counts as zero rows. A node whose
     * fragments fit yields one group, so a table under the bound fans
     * out exactly as before: one request per node.
     */
    public static List<FragmentGroup> splitByRows(
        Map<DiscoveryNode, List<Integer>> perNode,
        List<Integer> fragmentIds,
        List<Long> fragmentRows,
        long maxDocs
    ) {
        Map<Integer, Long> rowsById = new HashMap<>(fragmentIds.size());
        for (int i = 0; i < fragmentIds.size(); i++) {
            rowsById.put(fragmentIds.get(i), fragmentRows.get(i));
        }
        List<FragmentGroup> groups = new ArrayList<>(perNode.size());
        for (Map.Entry<DiscoveryNode, List<Integer>> assignment : perNode.entrySet()) {
            List<Integer> nodeFragments = assignment.getValue();
            long[] rows = new long[nodeFragments.size()];
            for (int i = 0; i < rows.length; i++) {
                rows[i] = rowsById.getOrDefault(nodeFragments.get(i), 0L);
            }
            int[] ends = LanceDirectoryReader.groupEnds(rows, maxDocs);
            int start = 0;
            for (int g = 0; g < ends.length; g++) {
                long groupRows = 0L;
                for (int i = start; i < ends[g]; i++) {
                    groupRows += rows[i];
                }
                groups.add(
                    new FragmentGroup(assignment.getKey(), List.copyOf(nodeFragments.subList(start, ends[g])), groupRows, g, ends.length)
                );
                start = ends[g];
            }
        }
        return groups;
    }

    /**
     * The override columns whose predicates never travel to Lance SQL:
     * {@code ip} (raw strings versus encoded doc values) and
     * {@code geo_point} (children hidden by the mapping).
     */
    public static Set<String> sqlExcludedColumns(LanceOverrides overrides) {
        Set<String> excluded = new LinkedHashSet<>(overrides.ipColumns());
        excluded.addAll(overrides.geoPointColumns().keySet());
        return excluded;
    }

    // ------------------------------------------------------------------
    // Fragment side: the scan's matched-count route.
    // ------------------------------------------------------------------

    /**
     * Result of {@link #computeMatched}: the number of matching rows
     * on this node, and whether counting stopped at the request's
     * {@code trackTotalHitsUpTo} bound so {@code value} is only a
     * lower bound of the true count.
     */
    public record MatchedCount(long value, boolean lowerBound) {
        public static final MatchedCount NOT_TRACKED = new MatchedCount(0L, false);

        public static MatchedCount exact(long value) {
            return new MatchedCount(value, false);
        }
    }

    /**
     * Count the live docs of {@code leaves} under each leaf's own
     * {@link LeafReader#getLiveDocs()} bitset. A reader wrapper's
     * (DLS) filtered liveDocs apply because the bits are read
     * directly; {@link LeafReader#numDocs()} would report the
     * unfiltered value, which is why a wrapped {@code match_all} count
     * goes through this instead of the {@link MatchAllDocsQuery} count
     * shortcut.
     */
    public static long countLiveDocs(List<LeafReaderContext> leaves) {
        long total = 0L;
        for (LeafReaderContext ctx : leaves) {
            LeafReader leaf = ctx.reader();
            Bits liveDocs = leaf.getLiveDocs();
            if (liveDocs == null) {
                total += leaf.numDocs();
            } else if (liveDocs instanceof FixedBitSet bits) {
                total += bits.cardinality();
            } else {
                int maxDoc = leaf.maxDoc();
                for (int doc = 0; doc < maxDoc; doc++) {
                    if (liveDocs.get(doc)) {
                        total++;
                    }
                }
            }
        }
        return total;
    }

    /**
     * Number of documents {@code query} matches on {@code searcher},
     * counted by collecting every doc its scorer yields. The searcher
     * hands the scorer each leaf's {@link LeafReader#getLiveDocs()} as
     * accepted docs, so a reader wrapper's filtered liveDocs apply.
     *
     * <p>{@link org.apache.lucene.search.IndexSearcher#count(Query)} is
     * not used because its {@code TotalHitCountCollector} asks
     * {@code Weight.count(LeafReaderContext)} first and takes that
     * answer without scoring: {@link MatchAllDocsQuery} answers
     * {@link LeafReader#numDocs()}, which the security plugin's DLS
     * leaf reader leaves at the unfiltered value, and other Weights
     * answer from index statistics that predate the wrapper as well.
     * The collector below never consults {@code Weight.count}, so the
     * only thing that decides the count is the scorer intersected with
     * the liveDocs.
     *
     * <p>The counter is a plain {@code long[]}: {@code search(Query, Collector)}
     * of the fragment searcher visits every leaf on the calling thread
     * whatever the searcher's slice count (a single collector cannot
     * be shared between slices), so the collector is never touched by
     * two threads. Counting through a
     * {@link org.apache.lucene.search.CollectorManager} would run the
     * slices in parallel and need one counter per slice.
     */
    public static long countThroughLiveDocs(ContextIndexSearcher searcher, Query query) throws IOException {
        long[] total = new long[1];
        searcher.search(query, new SimpleCollector() {
            @Override
            public void collect(int doc) {
                total[0]++;
            }

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }
        });
        return total[0];
    }

    /**
     * Determine the number of rows in this node's fragment subset
     * that satisfy the query, counted as far as the request's
     * {@link LanceFragmentQueryRequest#trackTotalHitsUpTo()} asks.
     * Uses Lance's metadata-only counting whenever the query is a
     * pure filter shape the coordinator's plan spells as Lance SQL
     * ({@code filterSql}, the plan's scalar filter; null for a scoring
     * query, an absent query or {@code match_all}):
     * <ul>
     *   <li>No filter: sum {@link Fragment#countRows()}
     *       across the assigned fragments (Lance metadata, no
     *       scan).</li>
     *   <li>Filter set, no fragment list: use
     *       {@link Dataset#countRows(String)}.</li>
     *   <li>Filter set, fragment list: {@link #countScalarFilter},
     *       which asks Lance to count the matches of the listed
     *       fragments natively when the request wants an accurate
     *       total and otherwise scans at most
     *       {@code trackTotalHitsUpTo + 1} rows to decide between an
     *       exact count and a lower bound.</li>
     * </ul>
     * The first two read Lance metadata or run one native count, so
     * they are cheap regardless of the match count and are reported
     * exact; the {@code track_total_hits} bound then only affects how
     * the coordinator presents them.
     *
     * <p>A bare {@link LanceFtsQuery} is counted from the Weight the
     * caller built for the request when that Weight's scan has run
     * (hits or aggregations were collected) and either was unbounded
     * or returned fewer rows than its {@code scanLimit}, since then
     * the scan saw every match. Otherwise the count comes from a
     * dedicated Lance scan that yields no payload columns
     * ({@link #countFtsHitsDirectly}), limited to
     * {@code trackTotalHitsUpTo + 1} rows unless the request asked for
     * an accurate total: reaching the limit proves there are more than
     * {@code trackTotalHitsUpTo} matches, which is all the
     * {@code gte} relation needs, and stops the scan from walking a
     * posting list whose length is what makes large FTS results slow.
     *
     * <p>For every other scoring shape (knn, a bool mixing FTS with
     * other scoring clauses, post_filter over any query) the
     * coordinator leaves filterSql null and only ships the
     * {@link QueryBuilder}. We cannot express those in Lance SQL, so
     * we ask Lucene through
     * {@link org.apache.lucene.search.IndexSearcher#count(Query)}
     * and report the exact number.
     *
     * <p>The {@code hasSecurityWrapper} flag overrides every
     * Lance-side fast path. A non-null reader wrapper on
     * {@code IndexService} indicates that DLS/FLS or another
     * reader-level transform may restrict the visible document
     * set; Lance's metadata-only counts and its native filter scan
     * see the raw Dataset, not the wrapper's view, so serving
     * {@code hits.total.value} from Lance would over-count and
     * disagree with the hits the same request returns. Whenever a
     * wrapper is installed the count is taken from the wrapped
     * leaves' {@link LeafReader#getLiveDocs()} instead, either
     * directly ({@link #countLiveDocs}, for {@code match_all}) or by
     * collecting the query's scorer under those liveDocs
     * ({@link #countThroughLiveDocs}), so the count matches the hits,
     * and {@code _count} (which takes this same path) agrees with
     * {@code _search}.
     */
    public static MatchedCount computeMatched(
        Dataset dataset,
        LanceFragmentQueryRequest request,
        String filterSql,
        ContextIndexSearcher searcher,
        Query luceneQuery,
        boolean hasSecurityWrapper,
        LanceFtsQuery.LanceFtsWeight ftsWeight,
        LanceCancellation cancellation
    ) throws Exception {
        int upTo = request.trackTotalHitsUpTo();
        if (upTo == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
            // track_total_hits: false. The coordinator leaves
            // hits.total out of the response, so no count is needed.
            return MatchedCount.NOT_TRACKED;
        }
        if (hasSecurityWrapper) {
            // A reader wrapper is installed on the IndexService,
            // most likely the security plugin's DLS/FLS wrapper.
            // Invariant of this branch: the count is derived from
            // the wrapped leaves' getLiveDocs(), either read directly
            // (countLiveDocs) or applied by the searcher while it
            // drives a scorer (countThroughLiveDocs). Nothing here
            // may read Lance metadata (Dataset.countRows,
            // Fragment.countRows), run a Lance count scan, or use a
            // Lucene shortcut that answers from LeafReader.numDocs():
            // all of those see the rows before the wrapper and would
            // report hidden rows in hits.total.value while the hits
            // themselves are filtered.
            //
            // The track_total_hits bound is not applied here. Both
            // paths below count every match, so the value is exact,
            // and exact is within the contract for any bound. For an
            // FTS query the Lance scan runs a second time here (the
            // hits phase's LanceFtsWeight and its shardHits are not
            // reused). That repeat is accepted: it is the only count
            // path that sees the wrapper's view, and DLS correctness
            // outranks the saving.
            //
            // match_all is counted from the liveDocs bitset. The
            // security plugin's DLS leaf reader swaps in filtered
            // liveDocs but leaves numDocs() at the unfiltered value,
            // and numDocs() is exactly what MatchAllDocsQuery's
            // Weight.count answers, so IndexSearcher.count must not
            // be used for it. MatchAllQueryBuilder produces an
            // ApproximateScoreQuery around the MatchAllDocsQuery;
            // once the hits phase has run, ContextIndexSearcher.rewrite
            // has called setContext on that instance and its rewrite
            // returns itself instead of the wrapped query, so unwrap
            // it explicitly before the ConstantScoreQuery
            // normalisation (which mirrors the first two lines of
            // IndexSearcher.count and catches a constant_score /
            // boost / bool-filter wrapper around match_all).
            Query normalised = luceneQuery instanceof ApproximateScoreQuery approximate ? approximate.getOriginalQuery() : luceneQuery;
            normalised = searcher.rewrite(new ConstantScoreQuery(normalised));
            if (normalised instanceof ConstantScoreQuery csq) {
                normalised = csq.getQuery();
            }
            if (normalised instanceof MatchAllDocsQuery) {
                return MatchedCount.exact(countLiveDocs(searcher.getIndexReader().leaves()));
            }
            return MatchedCount.exact(countThroughLiveDocs(searcher, luceneQuery));
        }
        List<Integer> fragmentIds = request.fragmentIdsOrNull();
        boolean hasScoringQuery = request.query() != null && filterSql == null;
        boolean hasPostFilter = request.postFilter() != null;
        if (hasScoringQuery && !hasPostFilter && luceneQuery instanceof LanceFtsQuery fts) {
            // Pure FTS shape (no post_filter, no other scoring
            // clause). A collapsed bool query arrives here as the
            // same LanceFtsQuery carrying its scalar clauses as
            // scanFilterSql, which every count path below applies
            // too.
            if (ftsWeight != null) {
                // The request's own Weight has scanned already when
                // hits or aggregations were collected. Its count is
                // the true total when the scan saw every match of
                // this executor's fragments: an unbounded scan, or a
                // bounded scan that came back short of its limit
                // before the fragment filter (Lance returns exactly
                // min(limit, matches) rows).
                long scanned = ftsWeight.hitCount();
                if (scanned >= 0 && ftsWeight.complete()) {
                    return MatchedCount.exact(scanned);
                }
            }
            if (upTo == SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
                return MatchedCount.exact(countFtsHitsDirectly(dataset, fts, fragmentIds, 0L, cancellation).own());
            }
            long limit = (long) upTo + 1L;
            FtsHitCount counted = countFtsHitsDirectly(dataset, fts, fragmentIds, limit, cancellation);
            // The bound is judged on the rows Lance returned before the
            // fragment filter. On a subset executor the own share of a
            // filled scan is a fraction of the limit and says nothing
            // about how many matches were cut off; the scan filling up
            // does, and it fills up on every executor at once, so each
            // reports a lower bound and the coordinator answers gte.
            return new MatchedCount(counted.own(), counted.scanned() >= limit);
        }
        if (hasScoringQuery || hasPostFilter) {
            // post_filter narrows hits.total.value below what
            // filterSql / countRows would return, so ask Lucene
            // directly against the AND-combined query. knn also
            // lands here (its LanceKnnQuery is not the LanceFtsQuery
            // branch above); Lucene serves the count via the shared
            // shard-level nearest scan the Weight already cached.
            return MatchedCount.exact(searcher.count(luceneQuery));
        }
        if (filterSql == null) {
            if (fragmentIds == null) {
                return MatchedCount.exact(dataset.countRows());
            }
            long total = 0L;
            List<Fragment> allFragments = dataset.getFragments();
            for (Fragment fragment : allFragments) {
                if (fragmentIds.contains(fragment.getId())) {
                    total += fragment.countRows();
                }
            }
            return MatchedCount.exact(total);
        }
        if (fragmentIds == null) {
            return MatchedCount.exact(dataset.countRows(filterSql));
        }
        return countScalarFilter(dataset, filterSql, fragmentIds, upTo, cancellation);
    }

    /**
     * Count the rows of {@code fragmentIds} that match the scalar
     * filter {@code filterSql}, honouring the {@code track_total_hits}
     * bound {@code upTo}.
     *
     * <p>The coordinator always hands an executor its fragment list,
     * so every scalar-filter count of the fragment path arrives here.
     * The scan asks Lance for zero payload columns and no row address
     * or row id, so nothing but a row count crosses from Lance to
     * Java. The fragment list is passed to Lance: for a scalar filter
     * it only narrows the fragments the filtered read opens (unlike
     * an FTS scan, where a fragment list turns into a prefilter over
     * {@code _rowid}), so there is no reason to scan the whole table
     * and sort the rows by fragment here.
     *
     * <ul>
     *   <li>{@code upTo == TRACK_TOTAL_HITS_ACCURATE}
     *       ({@code track_total_hits: true}, which is also what
     *       {@code _count} sends): {@link LanceScanner#countRows()}.
     *       Lance puts a {@code count(*)} on top of the filtered read
     *       and runs it across its own thread pool; the result comes
     *       back as one number. Pulling the same rows through
     *       {@code scanBatches()} instead would hand every match to
     *       this search thread one batch at a time, which for a
     *       filter that matches most of a large table costs seconds
     *       of a single core.</li>
     *   <li>Otherwise: one scan with {@code limit(upTo + 1)}, whose
     *       returned rows are counted. Lance plans the limit as a
     *       node above the filtered read, so the read stops once
     *       {@code upTo + 1} rows are through; at most that many rows
     *       reach Java. Reaching the limit proves the executor holds
     *       more than {@code upTo} matches, which is all the
     *       coordinator needs for {@code gte}, so the result is then
     *       a lower bound; coming back short means every match was
     *       seen and the count is exact. Because the scan is already
     *       restricted to the executor's fragments, every returned
     *       row is the executor's own and the returned count itself
     *       is compared with the limit. (The FTS counterpart judges
     *       on the rows before its fragment filter because that scan
     *       runs over the whole table.)</li>
     * </ul>
     *
     * <p>{@link LanceScanner#countRows()} is not used for the bounded
     * case: Lance applies its {@code count(*)} before the limit node,
     * so the limit would be ignored and the count would be exact at
     * full cost, which is what the bound exists to avoid.
     *
     * <p>{@link #NATIVE_SCALAR_COUNTS} and {@link #BOUNDED_SCALAR_COUNT_SCANS}
     * record which of the two paths ran, so a test can tell a native
     * count from a batch loop that happens to return the same number.
     */
    public static MatchedCount countScalarFilter(Dataset dataset, String filterSql, List<Integer> fragmentIds, int upTo) throws Exception {
        return countScalarFilter(dataset, filterSql, fragmentIds, upTo, LanceCancellation.NONE);
    }

    /**
     * {@link #countScalarFilter(Dataset, String, List, int)} whose bounded
     * scan stops once {@code cancellation} reports a cancelled task. The
     * native count of an exact request runs inside Lance in one call and
     * has no batch boundary to stop at.
     */
    public static MatchedCount countScalarFilter(
        Dataset dataset,
        String filterSql,
        List<Integer> fragmentIds,
        int upTo,
        LanceCancellation cancellation
    ) throws Exception {
        ScanOptions.Builder builder = countOnlyScan(filterSql, fragmentIds);
        if (upTo == SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
            cancellation.checkCancelled();
            try (LanceScanner scanner = dataset.newScan(builder.build())) {
                long counted = scanner.countRows();
                NATIVE_SCALAR_COUNTS.incrementAndGet();
                return MatchedCount.exact(counted);
            }
        }
        long limit = (long) upTo + 1L;
        long counted = countRows(dataset, builder.limit(limit).build(), cancellation);
        BOUNDED_SCALAR_COUNT_SCANS.incrementAndGet();
        return new MatchedCount(counted, counted >= limit);
    }

    /**
     * Number of scalar filter counts {@link #countScalarFilter} answered
     * through {@link LanceScanner#countRows()} since the class loaded.
     */
    public static final AtomicLong NATIVE_SCALAR_COUNTS = new AtomicLong();

    /**
     * Number of scalar filter counts {@link #countScalarFilter} answered
     * by a scan limited to {@code upTo + 1} rows since the class loaded.
     */
    public static final AtomicLong BOUNDED_SCALAR_COUNT_SCANS = new AtomicLong();

    /**
     * Scan options for a count-only scalar filter scan over
     * {@code fragmentIds}: no columns, no row address, no row id.
     */
    private static ScanOptions.Builder countOnlyScan(String filterSql, List<Integer> fragmentIds) {
        return new ScanOptions.Builder().filter(filterSql)
            .fragmentIds(fragmentIds)
            .columns(Collections.emptyList())
            .withRowAddress(false)
            .withRowId(false);
    }

    /**
     * Count FTS hits without materialising scores or payload columns.
     *
     * <p>Lance's inverted-index scanner can walk the posting list
     * once and stream row counts when we ask for zero columns and
     * no row address / row id. This is the count-only counterpart
     * of {@code Dataset.countRows(sqlFilter)} for scalar filters.
     * Without this path an FTS count goes through
     * {@code IndexSearcher.count(luceneQuery)}, which triggers
     * {@link LanceFtsQuery}'s Weight to materialise every match's
     * row address and score into a sparse array (see
     * {@code LanceFtsQuery.scorerSupplier}). The Weight is
     * necessary for the hits phase, but only wastes work for a
     * pure count.
     *
     * <p>The Lance SDK has no {@code Dataset.countRows(FullTextQuery)}
     * overload today, so this method assembles a scan that yields
     * zero payload columns; the batches carry only the row count
     * that the aggregator returns via {@code getRowCount()}. When
     * {@code fragmentIds} is null or covers every fragment of the
     * dataset that is the whole scan, and Lance answers it from the
     * inverted index alone.
     *
     * <p>A proper subset (one executor of a multi node fan out) is
     * not passed to Lance either, because a fragment list makes Lance
     * read {@code _rowid} over the listed fragments as a prefilter
     * (see {@link LanceFtsQuery#restrictToFragmentsUnlessAll}).
     * Instead the scan runs over the whole table with
     * {@code withRowAddress(true)} and the rows whose fragment id
     * (upper 32 bits of {@code _rowaddr}) is in {@code fragmentIds}
     * are counted here, next to the number of rows Lance returned
     * before that filter:
     * <ul>
     *   <li>{@code limit > 0} (the {@code track_total_hits} bound,
     *       passed as {@code upTo + 1}): one scan with that limit.
     *       Every executor sees the same {@code min(total, upTo + 1)}
     *       rows and counts its own fragments' share. The caller
     *       compares {@link FtsHitCount#scanned()} with the limit to
     *       decide whether the share is exact or a lower bound: when
     *       the scan filled its limit the table has more than
     *       {@code upTo} matches and every executor reports a lower
     *       bound, whatever its share. The shares themselves need
     *       not sum to {@code upTo + 1}, since a tie in score at the
     *       limit lets each executor's scan pick a different row.</li>
     *   <li>{@code limit == 0} ({@code track_total_hits: true}): a
     *       probe scan with {@code limit(effectiveSubsetProbeLimit)}
     *       for the rows the executor's fragments hold. When it
     *       comes back short every match has been seen and the
     *       executor's share is the exact count. When it fills up the
     *       probe is discarded and the count-only scan above runs with
     *       the {@code fragmentIds} restriction, paying the prefilter
     *       read for that one shape.</li>
     * </ul>
     *
     * <p>A prefilter carried by {@code fts} (the scalar clauses of a
     * collapsed bool query) is passed the same way the hits scan
     * passes it, so the count covers exactly the rows the hits phase
     * can return.
     *
     * <p>{@code limit} caps the rows the scan returns; {@code 0}
     * means no cap. Even with no payload columns the scan's cost
     * grows with the number of matches (Lance scores and ranks every
     * posting before it can emit rows), so a caller that only needs
     * to know whether more than {@code n} rows match passes
     * {@code n + 1} and stops the scan there.
     */
    public static FtsHitCount countFtsHitsDirectly(Dataset dataset, LanceFtsQuery fts, List<Integer> fragmentIds, long limit)
        throws Exception {
        return countFtsHitsDirectly(dataset, fts, fragmentIds, limit, LanceCancellation.NONE);
    }

    /** {@link #countFtsHitsDirectly(Dataset, LanceFtsQuery, List, long)} whose scans stop once {@code cancellation} reports a cancelled task. */
    public static FtsHitCount countFtsHitsDirectly(
        Dataset dataset,
        LanceFtsQuery fts,
        List<Integer> fragmentIds,
        long limit,
        LanceCancellation cancellation
    ) throws Exception {
        boolean subset = fragmentIds != null && !LanceFtsQuery.coversAllFragments(fragmentIds, dataset);
        if (!subset) {
            ScanOptions.Builder builder = countOnlyScan(fts);
            if (limit > 0) {
                builder = builder.limit(limit);
            }
            return FtsHitCount.whole(countRows(dataset, builder.build(), cancellation));
        }
        Set<Integer> own = new HashSet<>(fragmentIds);
        if (limit > 0) {
            return countOwnRows(dataset, rowAddressScan(fts).limit(limit).build(), own, cancellation);
        }
        long subsetRows = 0L;
        for (Fragment fragment : dataset.getFragments()) {
            if (own.contains(fragment.getId())) {
                subsetRows += fragment.countRows();
            }
        }
        long probeLimit = LanceFtsQuery.effectiveSubsetProbeLimit(subsetRows);
        FtsHitCount probe = countOwnRows(dataset, rowAddressScan(fts).limit(probeLimit).build(), own, cancellation);
        if (probe.scanned() < probeLimit) {
            return probe;
        }
        return FtsHitCount.whole(
            countRows(dataset, LanceFtsQuery.restrictToFragmentsUnlessAll(countOnlyScan(fts), fragmentIds, dataset).build(), cancellation)
        );
    }

    /** Scan options for a count-only FTS scan: no columns, no row address, no row id. */
    private static ScanOptions.Builder countOnlyScan(LanceFtsQuery fts) {
        ScanOptions.Builder builder = new ScanOptions.Builder().fullTextQuery(fts.fullTextQuery())
            .columns(Collections.emptyList())
            .withRowAddress(false)
            .withRowId(false);
        if (fts.scanFilterSql() != null) {
            builder = builder.filter(fts.scanFilterSql()).prefilter(true);
        }
        return builder;
    }

    /** Scan options for an FTS scan that returns {@code _rowaddr} only. */
    private static ScanOptions.Builder rowAddressScan(LanceFtsQuery fts) {
        return countOnlyScan(fts).withRowAddress(true);
    }

    private static long countRows(Dataset dataset, ScanOptions options, LanceCancellation cancellation) throws Exception {
        long total = 0L;
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                cancellation.checkCancelled();
                total += reader.getVectorSchemaRoot().getRowCount();
            }
        }
        return total;
    }

    /**
     * Result of {@link #countFtsHitsDirectly}: the rows Lance returned
     * for the scan before any fragment filter ({@code scanned}), and
     * how many of them belong to the fragments the executor holds
     * ({@code own}). The two are equal when the scan was already
     * restricted to those fragments or covered the whole table on an
     * executor that holds every fragment.
     */
    public record FtsHitCount(long scanned, long own) {
        /** A scan whose every returned row belongs to the executor. */
        public static FtsHitCount whole(long rows) {
            return new FtsHitCount(rows, rows);
        }
    }

    /**
     * Read {@code options} against {@code dataset} and count the rows
     * whose fragment id is in {@code own} next to every row returned.
     */
    private static FtsHitCount countOwnRows(Dataset dataset, ScanOptions options, Set<Integer> own, LanceCancellation cancellation)
        throws Exception {
        long scanned = 0L;
        long kept = 0L;
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                cancellation.checkCancelled();
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                int rows = root.getRowCount();
                scanned += rows;
                for (int i = 0; i < rows; i++) {
                    if (own.contains((int) (rowAddr.get(i) >>> 32))) {
                        kept++;
                    }
                }
            }
        }
        return new FtsHitCount(scanned, kept);
    }
}
