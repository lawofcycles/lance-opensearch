/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.Collections;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Weight;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.cache.query.DisabledQueryCache;
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
 * <p>Leaves are visited in reader order. {@link LanceFragmentSearchContext}
 * reports {@code shouldUseTimeSeriesDescSortOptimization() == false},
 * so the reversed traversal branch of the stock method is not needed.
 *
 * <p>The query cache is disabled: the Lance-backed reader has its own
 * freshness tracking and Lucene's per-query cache would only add
 * bookkeeping.
 */
final class LanceFragmentIndexSearcher extends ContextIndexSearcher {

    private static final QueryCachingPolicy NEVER_CACHE = new QueryCachingPolicy() {
        @Override
        public void onUse(Query query) {}

        @Override
        public boolean shouldCache(Query query) {
            return false;
        }
    };

    private final LanceFragmentSearchContext fragmentContext;

    LanceFragmentIndexSearcher(DirectoryReader reader, IndexSettings indexSettings, LanceFragmentSearchContext searchContext)
        throws IOException {
        super(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            new DisabledQueryCache(indexSettings),
            NEVER_CACHE,
            /* wrapWithExitableDirectoryReader */ false,
            /* executor */ null,
            searchContext
        );
        this.fragmentContext = searchContext;
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
     * mode the collector needs (a {@link org.apache.lucene.search.ScoreMode#COMPLETE}
     * Weight satisfies any collector).
     */
    void search(Weight weight, Collector collector) throws IOException {
        LeafReaderContextPartition[] partitions = (getLeafContexts() == null)
            ? new LeafReaderContextPartition[0]
            : getLeafContexts().stream().map(LeafReaderContextPartition::createForEntireSegment).toArray(LeafReaderContextPartition[]::new);
        search(partitions, weight, collector);
    }

    /**
     * {@link CollectorManager} counterpart of {@link #search(Weight, Collector)}.
     * The searcher has no executor, so a single collector covers every
     * leaf and {@code manager.reduce} sees exactly one collector, the
     * same shape {@link org.apache.lucene.search.IndexSearcher#search(Query, CollectorManager)}
     * produces here.
     */
    <C extends Collector, T> T search(Weight weight, CollectorManager<C, T> manager) throws IOException {
        C collector = manager.newCollector();
        search(weight, collector);
        return manager.reduce(Collections.singletonList(collector));
    }
}
