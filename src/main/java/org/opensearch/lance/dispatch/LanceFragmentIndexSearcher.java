/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.cache.query.DisabledQueryCache;
import org.opensearch.lance.engine.LanceCancellation;
import org.opensearch.lance.query.LanceHitsAccounting;
import org.opensearch.search.internal.ContextIndexSearcher;

/**
 * {@link ContextIndexSearcher} for the fragment path.
 *
 * <p>The stock {@link ContextIndexSearcher#search(LeafReaderContextPartition[], Weight, Collector)}
 * brackets every slice with
 * {@code searchContext.indexShard().getSearchOperationListener()}
 * callbacks. The fragment path runs on nodes that hold no shard copy
 * of the index, so {@link LanceFragmentSearchContext#indexShard()} is
 * {@code null} and those calls would fail. This subclass keeps the
 * rest of the slice loop (per-leaf {@link #searchLeaf} and the
 * {@code BucketCollectorProcessor#processPostCollection} step the
 * aggregation path depends on) and leaves the listener out. Nothing
 * that ships with OpenSearch implements the slice callbacks outside
 * the feature-flagged tiered storage slow log, and the fragment path
 * never reported into shard search stats, so no behaviour is lost.
 *
 * <p>Cancellation and timeout checks are not part of the skipped code.
 * They live in the inherited {@link #searchLeaf}, which calls
 * {@code cancellable.checkCancelled()} before collecting a leaf and
 * wraps the bulk scorer in {@code CancellableBulkScorer} when a
 * cancellation action is registered, and in the {@code QueryTimeout}
 * the constructor installs on the {@link IndexSearcher}. The stock slice
 * loop adds no check of its own between leaves, so this override does
 * not either.
 *
 * <p>Slices. The reader's leaves are the node's Lance fragments. When
 * {@link LanceFragmentSearchContext#getTargetMaxSliceCount()} is above
 * 1 the searcher is built with the executor's intra request pool
 * ({@code index_searcher}, never the SEARCH pool the request itself
 * executes on, whose queue admits requests) and the
 * inherited {@link ContextIndexSearcher#slices} bundles the leaves
 * into at most that many slices by row count (whole leaves, no
 * partition inside a fragment). Every {@link CollectorManager} search
 * ({@link #search(Query, CollectorManager)}, {@link #search(Weight, CollectorManager)},
 * and through them the top docs overloads of {@link IndexSearcher})
 * then collects one slice per task with its own collector: all but one
 * task are handed to the pool and the calling thread runs the rest,
 * the way Lucene's {@code TaskExecutor} does it, so a pool with no free
 * thread, or one that rejects the task, only reduces the parallelism to
 * the calling thread. The calling thread never waits for a task the
 * pool has not started, so a request whose search thread is itself a
 * pool thread cannot deadlock the pool. With a target of 1 the searcher
 * has no executor: Lucene then keeps one slice over every leaf in
 * reader order and runs it on the calling thread, which is the
 * behaviour the fragment path had before slicing and is what keeps a
 * one slice request identical, down to the order in which an
 * approximate aggregation sees its values, to the older one. Every
 * slice task checks the request's {@link LanceCancellation} before it
 * collects, and the inherited {@link #searchLeaf} checks it before
 * every leaf and inside the bulk scorer on whichever thread runs the
 * slice, so a cancelled task stops the pool threads as well as the
 * calling thread.
 *
 * <p>Leaves are visited in reader order within a slice.
 * {@link LanceFragmentSearchContext} reports
 * {@code shouldUseTimeSeriesDescSortOptimization() == false}, so the
 * reversed traversal branch of the stock method is not needed.
 *
 * <p>The query cache is disabled: the Lance-backed reader has its own
 * freshness tracking and Lucene's per-query cache would only add
 * bookkeeping.
 *
 * <p>The searcher owns the request's {@link LanceHitsAccounting}: every
 * Lance Weight created against it, whether by the executor directly or
 * by Lucene inside a {@code BooleanQuery} or under a reader wrapper,
 * reserves its hit buffers with the {@code request} breaker through
 * that one instance. The accounting is registered with the search
 * context as a releasable, so the bytes go back to the breaker when
 * the executor closes the context at the end of the request, after
 * the hits, aggregation and count phases that used the Weights.
 *
 * <p>The searcher also hands the Lance Weights the request's
 * {@link LanceCancellation} (from the search context), so their scans
 * stop at the next batch boundary once the task has been cancelled.
 */
final class LanceFragmentIndexSearcher extends ContextIndexSearcher implements LanceHitsAccounting.Provider, LanceCancellation.Provider {

    private static final QueryCachingPolicy NEVER_CACHE = new QueryCachingPolicy() {
        @Override
        public void onUse(Query query) {}

        @Override
        public boolean shouldCache(Query query) {
            return false;
        }
    };

    private final LanceFragmentSearchContext fragmentContext;
    private final LanceHitsAccounting hitsAccounting;

    /**
     * @param executor pool the slices after the first run on when
     *                 {@code searchContext.getTargetMaxSliceCount()} is
     *                 above 1; ignored (the searcher gets none) when it
     *                 is 1, see the class javadoc. May be null, which
     *                 collects on the calling thread whatever the slice
     *                 count says.
     */
    LanceFragmentIndexSearcher(
        DirectoryReader reader,
        IndexSettings indexSettings,
        LanceFragmentSearchContext searchContext,
        CircuitBreaker requestBreaker,
        Executor executor
    ) throws IOException {
        super(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            new DisabledQueryCache(indexSettings),
            NEVER_CACHE,
            /* wrapWithExitableDirectoryReader */ false,
            searchContext.getTargetMaxSliceCount() > 1 ? executor : null,
            searchContext
        );
        this.fragmentContext = searchContext;
        this.hitsAccounting = new LanceHitsAccounting(requestBreaker);
        searchContext.addReleasable(hitsAccounting);
    }

    @Override
    public LanceHitsAccounting hitsAccounting() {
        return hitsAccounting;
    }

    @Override
    public LanceCancellation cancellation() {
        return fragmentContext.cancellation();
    }

    @Override
    protected void search(LeafReaderContextPartition[] partitions, Weight weight, Collector collector) throws IOException {
        for (LeafReaderContextPartition partition : partitions) {
            searchLeaf(partition.ctx, partition.minDocId, partition.maxDocId, weight, collector);
        }
        fragmentContext.bucketCollectorProcessor().processPostCollection(collector);
    }

    /**
     * Run {@code collector} over every leaf with a {@link Weight} the
     * caller created, instead of a {@link Query} the searcher would
     * turn into a fresh Weight. {@code Weight}s of the Lance-backed
     * queries hold the result of one native Lance scan for the life of
     * the Weight, so a caller that drives hits, aggregations and the
     * match count off the same Weight runs that scan once per request
     * rather than once per phase. Mirrors
     * {@link ContextIndexSearcher#search(Query, Collector)} minus the
     * rewrite / createWeight steps; the caller is responsible for
     * having created the Weight against this searcher with the score
     * mode the collector needs (a {@link ScoreMode#COMPLETE} Weight
     * satisfies any collector).
     *
     * <p>A single collector cannot be shared between slices, so this
     * runs on the calling thread over every leaf whatever the slice
     * count, exactly like the inherited {@code search(Query, Collector)}.
     */
    void search(Weight weight, Collector collector) throws IOException {
        LeafReaderContextPartition[] partitions = (getLeafContexts() == null)
            ? new LeafReaderContextPartition[0]
            : getLeafContexts().stream().map(LeafReaderContextPartition::createForEntireSegment).toArray(LeafReaderContextPartition[]::new);
        search(partitions, weight, collector);
    }

    /**
     * Same steps as {@link IndexSearcher#search(Query, CollectorManager)}
     * (first collector, rewrite for its score mode, one Weight, then
     * one collector per slice), routed through
     * {@link #search(Weight, CollectorManager, Collector)} so the slice
     * loop is the one place that decides how slices run. Overridden
     * because the stock method's slice loop is private and the
     * fragment path needs two things it does not do: run the bucket
     * collector processor over a collector that saw no leaf at all (a
     * reader over an empty table still has to build its empty
     * aggregations), and start a Lance-backed Weight's shard scan once
     * before the slices begin.
     */
    @Override
    public <C extends Collector, T> T search(Query query, CollectorManager<C, T> manager) throws IOException {
        C firstCollector = manager.newCollector();
        Query rewritten = firstCollector.scoreMode().needsScores() ? rewrite(query) : rewrite(new ConstantScoreQuery(query));
        Weight weight = createWeight(rewritten, firstCollector.scoreMode(), 1f);
        return search(weight, manager, firstCollector);
    }

    /**
     * {@link CollectorManager} counterpart of {@link #search(Weight, Collector)}:
     * one collector per slice, the slices collected in parallel on the
     * executor when the searcher has one, then {@code manager.reduce}
     * over every collector. With no executor there is one slice and
     * {@code reduce} sees exactly one collector, the same shape
     * {@link IndexSearcher#search(Query, CollectorManager)} produces
     * on a searcher without an executor.
     */
    <C extends Collector, T> T search(Weight weight, CollectorManager<C, T> manager) throws IOException {
        return search(weight, manager, manager.newCollector());
    }

    /**
     * The slice loop. {@code firstCollector} is the collector the
     * caller already obtained from {@code manager} (Lucene creates it
     * before the Weight so the Weight can take the collector's score
     * mode); it collects the first slice.
     *
     * <p>Without a leaf (an empty table, or every fragment filtered
     * out) the collector is still run through
     * {@link #search(LeafReaderContextPartition[], Weight, Collector)}
     * with no partitions so that the bucket collector processor builds
     * the empty aggregations; Lucene's own loop returns before that
     * step and an aggregator tree would then have no result to read.
     *
     * <p>With several slices the Weight's scorer supplier is asked for
     * the first leaf on the calling thread before the tasks start.
     * A Lance-backed Weight ({@code LanceFtsQuery}, {@code LanceKnnQuery},
     * {@code LanceScanFilterQuery}, alone or inside a Boolean Weight)
     * runs its one shard level Lance scan on the first leaf it is asked
     * about and installs the result with a compare and set that every
     * later leaf reads; when the first ask came from each slice thread
     * at once, each thread ran the whole scan and all but one threw
     * theirs away. The early ask puts the scan on the calling thread,
     * once, and the slices find it installed. For any other Weight the
     * extra supplier is cheap and unused; Lucene allows asking for a
     * leaf's supplier more than once.
     */
    private <C extends Collector, T> T search(Weight weight, CollectorManager<C, T> manager, C firstCollector) throws IOException {
        LeafSlice[] slices = getSlices();
        if (slices.length == 0) {
            search(new LeafReaderContextPartition[0], weight, firstCollector);
            return manager.reduce(Collections.singletonList(firstCollector));
        }
        List<C> collectors = new ArrayList<>(slices.length);
        collectors.add(firstCollector);
        ScoreMode scoreMode = firstCollector.scoreMode();
        for (int i = 1; i < slices.length; i++) {
            C collector = manager.newCollector();
            if (collector.scoreMode() != scoreMode) {
                throw new IllegalStateException("CollectorManager does not always produce collectors with the same score mode");
            }
            collectors.add(collector);
        }
        if (slices.length > 1) {
            weight.scorerSupplier(slices[0].partitions[0].ctx);
        }
        List<Callable<C>> tasks = new ArrayList<>(slices.length);
        for (int i = 0; i < slices.length; i++) {
            LeafReaderContextPartition[] partitions = slices[i].partitions;
            C collector = collectors.get(i);
            tasks.add(() -> {
                // A slice that a pool thread picks up after the task was
                // cancelled stops here rather than collecting its leaves;
                // the inherited searchLeaf checks the same cancellation
                // before every leaf and inside the bulk scorer.
                fragmentContext.cancellation().checkCancelled();
                search(partitions, weight, collector);
                return collector;
            });
        }
        return manager.reduce(getTaskExecutor().invokeAll(tasks));
    }
}
