/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.Collector;
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
}
