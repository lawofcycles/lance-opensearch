/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.cache.query.DisabledQueryCache;
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
 * the collector tree to the bucket collector processor.
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

    public void testCountAndTopDocsWithoutIndexShard() throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir); LanceFragmentSearchContext context = newContext()) {
            assertNull(context.indexShard());
            LanceFragmentIndexSearcher searcher = new LanceFragmentIndexSearcher(reader, indexSettings, context);
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
            LanceFragmentIndexSearcher searcher = new LanceFragmentIndexSearcher(reader, indexSettings, context);
            CountingBucketCollector collector = new CountingBucketCollector();
            searcher.search(MatchAllDocsQuery.INSTANCE, collector);
            assertEquals(TOTAL, collector.collected);
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
            LanceFragmentIndexSearcher searcher = new LanceFragmentIndexSearcher(reader, indexSettings, context);
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
            assertEquals(TOTAL, collector.collected);
            assertTrue("collector tree must reach the bucket collector processor", processed.contains(collector));
        }
    }

    private static final class CountingBucketCollector extends BucketCollector {

        int collected;

        @Override
        public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) {
            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long owningBucketOrd) {
                    collected++;
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
