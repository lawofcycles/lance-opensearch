/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ipc.Query;
import org.lance.ipc.ScanOptions;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceFragmentLeafReader;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link LanceKnnQuery}: the Lucene {@code Query}
 * contract (equals, hashCode, toString, visitor), and the shape of the
 * shard level Lance scan its Weight issues against a real fixture
 * table (projection and hit buffer accounting).
 */
// The scan tests open a real Lance dataset; Lance JNI spins up native
// worker threads that outlive the test method.
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceKnnQueryTests extends OpenSearchTestCase {

    private static final String COLUMN = "embedding";
    private static final float[] VECTOR = new float[] { 0.1f, 0.2f, 0.3f, 0.4f };
    private static final int K = 5;

    public void testEqualsAndHashCodeAcrossEveryDimension() {
        LanceKnnQuery a = new LanceKnnQuery(COLUMN, VECTOR, K);
        LanceKnnQuery b = new LanceKnnQuery(COLUMN, new float[] { 0.1f, 0.2f, 0.3f, 0.4f }, K);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        // Column differs
        assertNotEquals(a, new LanceKnnQuery("other", VECTOR, K));
        // Vector differs
        assertNotEquals(a, new LanceKnnQuery(COLUMN, new float[] { 1.0f, 2.0f, 3.0f, 4.0f }, K));
        // k differs
        assertNotEquals(a, new LanceKnnQuery(COLUMN, VECTOR, K + 1));
        // Vector length differs
        assertNotEquals(a, new LanceKnnQuery(COLUMN, new float[] { 0.1f, 0.2f }, K));
    }

    public void testToStringShowsColumnAndK() {
        String text = new LanceKnnQuery(COLUMN, VECTOR, K).toString(COLUMN);
        assertTrue("toString should mention the column, saw: " + text, text.contains(COLUMN));
        assertTrue("toString should mention k, saw: " + text, text.contains("k=" + K));
    }

    public void testVisitorReceivesLeaf() {
        LanceKnnQuery query = new LanceKnnQuery(COLUMN, VECTOR, K);
        AtomicInteger leafCalls = new AtomicInteger();
        query.visit(new QueryVisitor() {
            @Override
            public void visitLeaf(org.apache.lucene.search.Query q) {
                leafCalls.incrementAndGet();
                assertSame(query, q);
            }
        });
        assertEquals("expected exactly one visitLeaf call", 1, leafCalls.get());
    }

    public void testNearestScanReturnsRowAddressAndDistanceOnly() throws Exception {
        // Three fragments of 50 rows, embedding[0] = id. The three
        // nearest rows of (75.4, 0, ...) are 75, 76 and 74, all in
        // fragment 1 at offsets 25, 26, 24. The scan the Weight issues
        // projects _distance and asks for the row address, so Lance
        // returns those two columns and nothing else.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(scratchDir, "knn-projection", 3, 50);
        float[] vector = new float[8];
        vector[0] = 75.4f;
        try (LanceDirectoryReader reader = openReader(uri)) {
            assertEquals(3, reader.leaves().size());
            IndexSearcher searcher = new IndexSearcher(reader);
            Weight weight = searcher.createWeight(new LanceKnnQuery(COLUMN, vector, 3), ScoreMode.COMPLETE, 1f);
            assertEquals(List.of(24, 25, 26), docIdsOn(weight, reader.leaves().get(1)));
            assertEquals(List.of(), docIdsOn(weight, reader.leaves().get(0)));

            ScanOptions issued = new ScanOptions.Builder().nearest(new Query.Builder().setColumn(COLUMN).setKey(vector).setK(3).build())
                .columns(LanceKnnQuery.HITS_SCAN_COLUMNS)
                .withRowAddress(true)
                .build();
            Dataset dataset = LanceFragmentLeafReader.unwrap(reader.leaves().get(0).reader()).dataset();
            assertEquals(Set.of("_rowaddr", "_distance"), LanceFtsQueryTests.columnsReturnedBy(dataset, issued));
        }
    }

    public void testNearestHitBuffersAreReservedAndReleasedOnClose() throws Exception {
        // Same fixture and query: the k = 3 rows land in one fragment,
        // so one buffer at the initial capacity is reserved by the
        // scan and the sorted view of that leaf on top of it. Closing
        // the accounting returns everything.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(scratchDir, "knn-accounting", 3, 50);
        float[] vector = new float[8];
        vector[0] = 75.4f;
        CircuitBreaker breaker = LanceFtsQueryTests.requestBreaker("1mb");
        try (
            LanceDirectoryReader reader = openReader(uri);
            LanceFtsQueryTests.AccountingSearcher searcher = new LanceFtsQueryTests.AccountingSearcher(reader, breaker)
        ) {
            Weight weight = searcher.createWeight(new LanceKnnQuery(COLUMN, vector, 3), ScoreMode.COMPLETE, 1f);
            assertEquals(0L, breaker.getUsed());
            assertEquals(List.of(), docIdsOn(weight, reader.leaves().get(0)));
            long oneBuffer = (long) LanceFragmentHits.INITIAL_CAPACITY * LanceFragmentHits.BYTES_PER_HIT;
            assertEquals("one fragment carries hits; the empty leaf sorts nothing", oneBuffer, breaker.getUsed());
            assertEquals(List.of(24, 25, 26), docIdsOn(weight, reader.leaves().get(1)));
            assertEquals(oneBuffer + 3L * LanceFragmentHits.BYTES_PER_HIT, breaker.getUsed());
            assertEquals(breaker.getUsed(), searcher.accounting.reservedBytes());

            searcher.accounting.close();
            assertEquals(0L, breaker.getUsed());
        }
    }

    /** Open a reader over every fragment of the table at {@code uri}; the reader owns the Dataset. */
    private static LanceDirectoryReader openReader(String uri) throws Exception {
        Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty());
        List<Integer> fragmentIds = new ArrayList<>();
        for (Fragment fragment : dataset.getFragments()) {
            fragmentIds.add(fragment.getId());
        }
        return LanceDirectoryReader.openForFragments(
            new ByteBuffersDirectory(),
            null,
            dataset,
            "",
            LanceEngineFactory.LancePrimaryKeyType.NONE,
            LanceOverrides.EMPTY,
            fragmentIds
        );
    }

    /** Doc ids the Weight's scorer yields on {@code leaf}, in iteration order. */
    private static List<Integer> docIdsOn(Weight weight, LeafReaderContext leaf) throws Exception {
        List<Integer> docIds = new ArrayList<>();
        ScorerSupplier supplier = weight.scorerSupplier(leaf);
        DocIdSetIterator iterator = supplier.get(Long.MAX_VALUE).iterator();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            docIds.add(doc);
        }
        return docIds;
    }
}
