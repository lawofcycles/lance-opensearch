/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.cache.query.DisabledQueryCache;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.search.aggregations.BucketCollector;
import org.opensearch.search.aggregations.BucketCollectorProcessor;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link LanceFragmentIndexSearcher} against a {@link LanceFragmentSearchContext}
 * whose {@code indexShard()} is null, which is the state on a data node
 * that holds no shard copy of the index. The stock
 * {@link ContextIndexSearcher} fetches the search operation listener
 * through {@code indexShard()} on every slice and fails there; the
 * subclass has to run the same slice loop without it and still hand
 * the collector tree to the bucket collector processor. The slice
 * tests check that a slice count above 1 cuts the leaves into several
 * slices with one collector each, that the collectors' reduce yields
 * what one collector over every leaf yields, and that a pool that
 * refuses the slice tasks leaves the whole search to the calling
 * thread.
 */
public class LanceFragmentIndexSearcherTests extends OpenSearchTestCase {

    private static final int TOTAL = 5;

    private Directory dir;
    private IndexSettings indexSettings;
    private ShardId shardId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dir = new ByteBuffersDirectory();
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < TOTAL; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
                doc.add(new NumericDocValuesField("n", i));
                writer.addDocument(doc);
                if (i % 2 == 1) {
                    // Several segments so the searcher visits more than one leaf.
                    writer.commit();
                }
            }
        }
        indexSettings = IndexSettingsModule.newIndexSettings("lance-test", Settings.EMPTY);
        shardId = new ShardId(indexSettings.getIndex(), 0);
    }

    @Override
    public void tearDown() throws Exception {
        dir.close();
        super.tearDown();
    }

    private LanceFragmentSearchContext newContext() {
        return new LanceFragmentSearchContext(
            shardId,
            /* mapperService */ null,
            MatchAllDocsQuery.INSTANCE,
            /* aggregations */ null,
            BigArrays.NON_RECYCLING_INSTANCE,
            /* bitsetFilterCache */ null,
            "node-a"
        );
    }

    private LanceFragmentIndexSearcher newSearcher(DirectoryReader reader, LanceFragmentSearchContext context, Executor executor)
        throws IOException {
        return new LanceFragmentIndexSearcher(reader, indexSettings, context, new NoopCircuitBreaker(CircuitBreaker.REQUEST), executor);
    }

    public void testCountAndTopDocsWithoutIndexShard() throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir); LanceFragmentSearchContext context = newContext()) {
            assertNull(context.indexShard());
            LanceFragmentIndexSearcher searcher = newSearcher(reader, context, null);
            assertTrue("fixture must span several leaves", reader.leaves().size() > 1);
            assertEquals(TOTAL, searcher.count(MatchAllDocsQuery.INSTANCE));
            TopDocs top = searcher.search(MatchAllDocsQuery.INSTANCE, 3, new Sort(new SortField("n", SortField.Type.LONG, true)));
            assertEquals(TOTAL, top.totalHits.value());
            assertEquals(3, top.scoreDocs.length);
        }
    }

    public void testStockSearcherNeedsIndexShard() throws IOException {
        // Pins the reason the subclass exists: the stock slice loop
        // dereferences searchContext.indexShard().
        try (DirectoryReader reader = DirectoryReader.open(dir); LanceFragmentSearchContext context = newContext()) {
            ContextIndexSearcher stock = new ContextIndexSearcher(
                reader,
                IndexSearcher.getDefaultSimilarity(),
                new DisabledQueryCache(indexSettings),
                new QueryCachingPolicy() {
                    @Override
                    public void onUse(Query query) {}

                    @Override
                    public boolean shouldCache(Query query) {
                        return false;
                    }
                },
                false,
                null,
                context
            );
            expectThrows(NullPointerException.class, () -> stock.count(MatchAllDocsQuery.INSTANCE));
        }
    }

    public void testPostCollectionRunsOnceOnTheCollectorTree() throws IOException {
        // The aggregation path relies on the slice loop ending with
        // BucketCollectorProcessor.processPostCollection over the
        // collector tree that was handed to search().
        try (DirectoryReader reader = DirectoryReader.open(dir); LanceFragmentSearchContext context = newContext()) {
            List<Collector> processed = new ArrayList<>();
            context.setBucketCollectorProcessor(new BucketCollectorProcessor() {
                @Override
                public void processPostCollection(Collector collectorTree) throws IOException {
                    processed.add(collectorTree);
                    super.processPostCollection(collectorTree);
                }
            });
            LanceFragmentIndexSearcher searcher = newSearcher(reader, context, null);
            CountingBucketCollector collector = new CountingBucketCollector();
            searcher.search(MatchAllDocsQuery.INSTANCE, collector);
            assertEquals(TOTAL, collector.collected.get());
            assertEquals(List.of(collector), processed);
        }
    }

    public void testWeightDrivenSearchMatchesQueryDrivenSearch() throws IOException {
        // The executor builds one Weight per request and drives hits,
        // aggregations and the count through it. The Weight entry
        // points have to visit every leaf, hand the collector tree to
        // the bucket collector processor once, and reduce to the same
        // page the Query entry points produce.
        try (DirectoryReader reader = DirectoryReader.open(dir); LanceFragmentSearchContext context = newContext()) {
            List<Collector> processed = new ArrayList<>();
            context.setBucketCollectorProcessor(new BucketCollectorProcessor() {
                @Override
                public void processPostCollection(Collector collectorTree) throws IOException {
                    processed.add(collectorTree);
                    super.processPostCollection(collectorTree);
                }
            });
            LanceFragmentIndexSearcher searcher = newSearcher(reader, context, null);
            Weight weight = searcher.createWeight(searcher.rewrite(MatchAllDocsQuery.INSTANCE), ScoreMode.COMPLETE, 1f);

            Sort sort = new Sort(new SortField("n", SortField.Type.LONG, true));
            TopFieldDocs viaWeight = searcher.search(weight, new TopFieldCollectorManager(sort, 3, null, Integer.MAX_VALUE));
            TopFieldDocs viaQuery = searcher.search(MatchAllDocsQuery.INSTANCE, 3, sort);
            assertEquals(TOTAL, viaWeight.totalHits.value());
            assertEquals(viaQuery.scoreDocs.length, viaWeight.scoreDocs.length);
            for (int i = 0; i < viaQuery.scoreDocs.length; i++) {
                assertEquals("doc at rank " + i, viaQuery.scoreDocs[i].doc, viaWeight.scoreDocs[i].doc);
            }

            CountingBucketCollector collector = new CountingBucketCollector();
            searcher.search(weight, collector);
            assertEquals(TOTAL, collector.collected.get());
            assertTrue("collector tree must reach the bucket collector processor", processed.contains(collector));
        }
    }

    public void testOneSliceKeepsOneCollectorOnTheCallingThread() throws Exception {
        // Slice count 1 is the pre-slicing behaviour: no executor is
        // used even when one is given, a single collector covers every
        // leaf in reader order, and the calling thread does the work.
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (DirectoryReader reader = DirectoryReader.open(dir); LanceFragmentSearchContext context = newContext()) {
            context.withTargetMaxSliceCount(1);
            assertFalse(context.shouldUseConcurrentSearch());
            LanceFragmentIndexSearcher searcher = newSearcher(reader, context, pool);
            assertEquals(1, searcher.getSlices().length);
            assertEquals(reader.leaves().size(), searcher.getSlices()[0].partitions.length);
            for (int i = 0; i < reader.leaves().size(); i++) {
                assertSame("leaf order inside the single slice", reader.leaves().get(i), searcher.getSlices()[0].partitions[i].ctx);
            }
            CountingManager manager = new CountingManager();
            assertEquals(TOTAL, searcher.search(MatchAllDocsQuery.INSTANCE, manager).intValue());
            assertEquals(1, manager.created.get());
            assertEquals(Set.of(Thread.currentThread().getName()), manager.threads);
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testSeveralSlicesCollectWithOneCollectorEachAndReduceToTheSameAnswer() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            assertTrue("fixture must span several leaves", reader.leaves().size() > 1);
            Sort sort = new Sort(new SortField("n", SortField.Type.LONG, true));

            TopFieldDocs oneSlice;
            int oneSliceCount;
            try (LanceFragmentSearchContext context = newContext()) {
                LanceFragmentIndexSearcher searcher = newSearcher(reader, context.withTargetMaxSliceCount(1), pool);
                oneSlice = searcher.search(MatchAllDocsQuery.INSTANCE, new TopFieldCollectorManager(sort, 3, null, Integer.MAX_VALUE));
                oneSliceCount = searcher.search(MatchAllDocsQuery.INSTANCE, new CountingManager());
            }

            try (LanceFragmentSearchContext context = newContext()) {
                context.withTargetMaxSliceCount(4);
                assertTrue(context.shouldUseConcurrentSearch());
                LanceFragmentIndexSearcher searcher = newSearcher(reader, context, pool);
                // Three leaves, target four: one slice per leaf.
                assertEquals(reader.leaves().size(), searcher.getSlices().length);

                List<Collector> processed = new ArrayList<>();
                context.setBucketCollectorProcessor(new BucketCollectorProcessor() {
                    @Override
                    public void processPostCollection(Collector collectorTree) throws IOException {
                        synchronized (processed) {
                            processed.add(collectorTree);
                        }
                        super.processPostCollection(collectorTree);
                    }
                });
                CountingManager manager = new CountingManager();
                assertEquals(oneSliceCount, searcher.search(MatchAllDocsQuery.INSTANCE, manager).intValue());
                assertEquals("one collector per slice", searcher.getSlices().length, manager.created.get());
                assertEquals("every slice's collector went through post collection", manager.created.get(), processed.size());

                // The slices really ran on several threads: a manager
                // whose collectors wait for each other at a barrier can
                // only complete when every slice collects at once, which
                // the calling thread alone could never do. A silent fall
                // back to the calling thread would time out here.
                CountingManager barrier = new CountingManager(new CyclicBarrier(searcher.getSlices().length));
                assertEquals(TOTAL, searcher.search(MatchAllDocsQuery.INSTANCE, barrier).intValue());
                assertEquals(searcher.getSlices().length, barrier.threads.size());
                assertTrue("a pool thread collected: " + barrier.threads, barrier.threads.size() > 1);
                Set<String> others = new HashSet<>(barrier.threads);
                others.remove(Thread.currentThread().getName());
                assertFalse("a thread other than the caller collected: " + barrier.threads, others.isEmpty());

                TopFieldDocs sliced = searcher.search(
                    MatchAllDocsQuery.INSTANCE,
                    new TopFieldCollectorManager(sort, 3, null, Integer.MAX_VALUE)
                );
                assertEquals(oneSlice.totalHits.value(), sliced.totalHits.value());
                assertEquals(oneSlice.scoreDocs.length, sliced.scoreDocs.length);
                for (int i = 0; i < oneSlice.scoreDocs.length; i++) {
                    assertEquals("doc at rank " + i, oneSlice.scoreDocs[i].doc, sliced.scoreDocs[i].doc);
                }

                // The Weight entry point takes the same slice loop.
                Weight weight = searcher.createWeight(searcher.rewrite(MatchAllDocsQuery.INSTANCE), ScoreMode.COMPLETE, 1f);
                CountingManager viaWeight = new CountingManager();
                assertEquals(TOTAL, searcher.search(weight, viaWeight).intValue());
                assertEquals(searcher.getSlices().length, viaWeight.created.get());
            }
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testRejectingPoolLeavesEverySliceToTheCallingThread() throws IOException {
        // The SEARCH pool refuses a task when its queue is full. The
        // slice loop must then run the refused slices itself rather
        // than fail the request or wait for a thread that never comes.
        AtomicInteger rejected = new AtomicInteger();
        Executor refusing = task -> {
            rejected.incrementAndGet();
            throw new RejectedExecutionException("pool full");
        };
        try (DirectoryReader reader = DirectoryReader.open(dir); LanceFragmentSearchContext context = newContext()) {
            LanceFragmentIndexSearcher searcher = newSearcher(reader, context.withTargetMaxSliceCount(4), refusing);
            assertEquals(reader.leaves().size(), searcher.getSlices().length);
            CountingManager manager = new CountingManager();
            assertEquals(TOTAL, searcher.search(MatchAllDocsQuery.INSTANCE, manager).intValue());
            assertEquals(searcher.getSlices().length, manager.created.get());
            assertEquals("all but one slice were offered to the pool", searcher.getSlices().length - 1, rejected.get());
            assertEquals(Set.of(Thread.currentThread().getName()), manager.threads);
        }
    }

    public void testEmptyReaderStillRunsPostCollection() throws IOException {
        // An executor over a table with no rows opens a reader without
        // leaves; the aggregation path still has to build its empty
        // aggregations, which happens in processPostCollection.
        try (Directory empty = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(empty, new IndexWriterConfig())) {
                writer.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(empty); LanceFragmentSearchContext context = newContext()) {
                assertTrue(reader.leaves().isEmpty());
                List<Collector> processed = new ArrayList<>();
                context.setBucketCollectorProcessor(new BucketCollectorProcessor() {
                    @Override
                    public void processPostCollection(Collector collectorTree) throws IOException {
                        processed.add(collectorTree);
                        super.processPostCollection(collectorTree);
                    }
                });
                LanceFragmentIndexSearcher searcher = newSearcher(reader, context.withTargetMaxSliceCount(4), null);
                CountingManager manager = new CountingManager();
                assertEquals(0, searcher.search(MatchAllDocsQuery.INSTANCE, manager).intValue());
                assertEquals(1, manager.created.get());
                assertEquals(1, processed.size());
            }
        }
    }

    public void testPartialOnShardIsSliceLevelOnlyWithSeveralSlices() {
        try (LanceFragmentSearchContext context = newContext()) {
            assertFalse(context.withTargetMaxSliceCount(1).partialOnShard().isSliceLevel());
            assertFalse(context.partialOnShard().isFinalReduce());
            // Several slices without a script service: the slice level
            // reduce must not run with a null service, so the context
            // refuses to build its reduce context.
            IllegalStateException refused = expectThrows(
                IllegalStateException.class,
                () -> context.withTargetMaxSliceCount(2).partialOnShard()
            );
            assertTrue(refused.getMessage(), refused.getMessage().contains("withScriptService"));
            context.withScriptService(new ScriptService(Settings.EMPTY, Map.of(), ScriptModule.CORE_CONTEXTS));
            assertTrue(context.partialOnShard().isSliceLevel());
            assertFalse(context.partialOnShard().isFinalReduce());
        }
    }

    /**
     * One {@link CountingBucketCollector} per {@code newCollector} call;
     * {@code reduce} sums their counts. Records the threads that
     * collected so a test can tell where the slices ran. With a
     * {@link CyclicBarrier}, every collector waits at its first leaf
     * until as many collectors have reached theirs, which only several
     * threads collecting at once can satisfy.
     */
    private static final class CountingManager implements CollectorManager<CountingBucketCollector, Integer> {
        final AtomicInteger created = new AtomicInteger();
        final Set<String> threads = ConcurrentHashMap.newKeySet();
        private final CyclicBarrier barrier;

        CountingManager() {
            this(null);
        }

        CountingManager(CyclicBarrier barrier) {
            this.barrier = barrier;
        }

        @Override
        public CountingBucketCollector newCollector() {
            created.incrementAndGet();
            return new CountingBucketCollector(threads, barrier);
        }

        @Override
        public Integer reduce(Collection<CountingBucketCollector> collectors) {
            int total = 0;
            for (CountingBucketCollector collector : collectors) {
                total += collector.collected.get();
            }
            return total;
        }
    }

    private static final class CountingBucketCollector extends BucketCollector {

        final AtomicInteger collected = new AtomicInteger();
        private final Set<String> threads;
        private final CyclicBarrier barrier;
        private boolean waited;

        CountingBucketCollector() {
            this(ConcurrentHashMap.newKeySet(), null);
        }

        CountingBucketCollector(Set<String> threads, CyclicBarrier barrier) {
            this.threads = threads;
            this.barrier = barrier;
        }

        @Override
        public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
            threads.add(Thread.currentThread().getName());
            if (barrier != null && !waited) {
                waited = true;
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    throw new IOException("the slices did not collect at the same time", e);
                }
            }
            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long owningBucketOrd) {
                    collected.incrementAndGet();
                }
            };
        }

        @Override
        public void preCollection() {}

        @Override
        public void postCollection() {}

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }
}
