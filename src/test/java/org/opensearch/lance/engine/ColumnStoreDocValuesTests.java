/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.memory.RootAllocator;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.lance.Dataset;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.lance.LanceCircuitBreaker;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.engine.LanceWarmCache.Lease;
import org.opensearch.lance.engine.LanceWarmCache.Snapshot;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Off-heap numeric and boolean doc values served from {@link ColumnStore}
 * through {@link LanceDirectoryReader#openForSnapshot}: every value and
 * presence bit equals the heap {@code long[]} path, one scan per
 * (snapshot, column), pins block eviction, the budget fallback loads into
 * heap, deleted rows are masked, and the breaker sees the footprint.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ColumnStoreDocValuesTests extends OpenSearchTestCase {

    private static final int FRAGMENTS = 3;
    private static final int ROWS = 200;
    private static final String UUID = "column-store-uuid";

    private RootAllocator allocator;
    private LanceWarmCache cache;
    private String uri;
    private List<Integer> allFragments;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Path scratchDir = createTempDir();
        uri = LanceTableFactory.writeHintFixtureTable(scratchDir, "store-" + getTestName(), FRAGMENTS, ROWS);
        allocator = new RootAllocator(Long.MAX_VALUE);
        cache = new LanceWarmCache(allocator, 64L * 1024 * 1024, 64, true);
        allFragments = new ArrayList<>();
        for (int f = 0; f < FRAGMENTS; f++) {
            allFragments.add(f);
        }
        LanceCircuitBreaker.setBreaker(null);
    }

    @Override
    public void tearDown() throws Exception {
        LanceCircuitBreaker.setBreaker(null);
        LanceCircuitBreaker.setEnabled(true);
        if (cache != null) {
            cache.close();
        }
        if (allocator != null) {
            allocator.close();
        }
        super.tearDown();
    }

    private Lease acquire() throws Exception {
        return cache.acquire(UUID, uri, StorageOptions.empty(), Optional.empty(), "", LancePrimaryKeyType.NONE, Collections.emptyMap());
    }

    private LanceDirectoryReader openCached(Snapshot snapshot, List<Integer> fragmentIds) throws IOException {
        return LanceDirectoryReader.openForSnapshot(new ByteBuffersDirectory(), snapshot, cache.columnStore(), fragmentIds, null);
    }

    /** Heap reference: the same snapshot without a store, so every column loads into {@code long[]}. */
    private LanceDirectoryReader openHeap(Snapshot snapshot, List<Integer> fragmentIds) throws IOException {
        return LanceDirectoryReader.openForSnapshot(new ByteBuffersDirectory(), snapshot, null, fragmentIds, null);
    }

    private static List<LanceFragmentLeafReader> leavesOf(LanceDirectoryReader reader) {
        List<LanceFragmentLeafReader> leaves = new ArrayList<>();
        for (LeafReaderContext ctx : reader.leaves()) {
            leaves.add(LanceFragmentLeafReader.unwrap(ctx.reader()));
        }
        return leaves;
    }

    /** Values of {@code column} on {@code leaf} read with advanceExact over every doc; {@code null} marks a missing value. */
    private static Long[] readByAdvanceExact(LanceFragmentLeafReader leaf, String column) throws IOException {
        NumericDocValues values = leaf.getNumericDocValues(column);
        Long[] out = new Long[leaf.maxDoc()];
        for (int doc = 0; doc < leaf.maxDoc(); doc++) {
            out[doc] = values.advanceExact(doc) ? values.longValue() : null;
        }
        return out;
    }

    /** Doc ids and values {@code column} yields when iterated with nextDoc. */
    private static List<long[]> readByIteration(LanceFragmentLeafReader leaf, String column) throws IOException {
        NumericDocValues values = leaf.getNumericDocValues(column);
        List<long[]> out = new ArrayList<>();
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            out.add(new long[] { doc, values.longValue() });
        }
        return out;
    }

    private static void assertSameColumn(String column, LanceFragmentLeafReader heap, LanceFragmentLeafReader offHeap) throws IOException {
        Long[] expected = readByAdvanceExact(heap, column);
        Long[] actual = readByAdvanceExact(offHeap, column);
        assertArrayEquals(column + " on fragment " + heap.fragmentId(), expected, actual);
        List<long[]> expectedIteration = readByIteration(heap, column);
        List<long[]> actualIteration = readByIteration(offHeap, column);
        assertEquals(column + " iteration length on fragment " + heap.fragmentId(), expectedIteration.size(), actualIteration.size());
        for (int i = 0; i < expectedIteration.size(); i++) {
            assertArrayEquals(column + " iteration on fragment " + heap.fragmentId(), expectedIteration.get(i), actualIteration.get(i));
        }
        assertTrue(heap.isColumnFullyLoaded(column));
        assertFalse("the reference reader has no store", heap.isServingOffHeap(column));
    }

    private static Long rating(int i) {
        return i % 5 == 4 ? null : (long) ((i * 37) % 1000);
    }

    public void testOffHeapValuesEqualTheHeapColumnOnEveryDoc() throws Exception {
        try (Lease lease = acquire()) {
            Snapshot snapshot = lease.snapshot();
            try (
                LanceDirectoryReader offHeap = openCached(snapshot, allFragments);
                LanceDirectoryReader heap = openHeap(snapshot, allFragments)
            ) {
                List<LanceFragmentLeafReader> offHeapLeaves = leavesOf(offHeap);
                List<LanceFragmentLeafReader> heapLeaves = leavesOf(heap);
                for (int f = 0; f < FRAGMENTS; f++) {
                    assertSameColumn("rating", heapLeaves.get(f), offHeapLeaves.get(f));
                    assertSameColumn("flag", heapLeaves.get(f), offHeapLeaves.get(f));
                    assertSameColumn("id", heapLeaves.get(f), offHeapLeaves.get(f));
                    assertTrue(offHeapLeaves.get(f).isServingOffHeap("rating"));
                    assertTrue(offHeapLeaves.get(f).isServingOffHeap("flag"));
                    assertTrue(offHeapLeaves.get(f).isColumnFullyLoaded("rating"));
                }
                // Spot check against the fixture formula as well.
                Long[] fragment1 = readByAdvanceExact(offHeapLeaves.get(1), "rating");
                for (int offset = 0; offset < ROWS; offset++) {
                    assertEquals("rating of row " + (ROWS + offset), rating(ROWS + offset), fragment1[offset]);
                }
                ColumnStore store = cache.columnStore();
                assertEquals("one scan per column", 3L, store.loadCount());
                assertEquals(FRAGMENTS * 3, store.entryCount());
                assertTrue(store.allocatedBytes() > 0L);
                assertEquals(store.allocatedBytes(), cache.columnCacheBytes());
            }
        }
    }

    public void testSecondReaderReadsTheStoreWithoutAScan() throws Exception {
        ColumnStore store = cache.columnStore();
        try (Lease lease = acquire()) {
            try (LanceDirectoryReader first = openCached(lease.snapshot(), allFragments)) {
                readByAdvanceExact(leavesOf(first).get(0), "rating");
                assertEquals(1L, store.loadCount());
                assertEquals(0L, store.hitCount());
            }
        }
        long bytesAfterFirst = store.allocatedBytes();
        try (Lease lease = acquire()) {
            try (LanceDirectoryReader second = openCached(lease.snapshot(), allFragments)) {
                for (LanceFragmentLeafReader leaf : leavesOf(second)) {
                    Long[] values = readByAdvanceExact(leaf, "rating");
                    for (int offset = 0; offset < ROWS; offset++) {
                        assertEquals(rating(leaf.fragmentId() * ROWS + offset), values[offset]);
                    }
                    assertTrue(leaf.isServingOffHeap("rating"));
                }
                assertEquals("no second scan", 1L, store.loadCount());
                assertEquals(1L, store.hitCount());
                assertEquals("no new allocation", bytesAfterFirst, store.allocatedBytes());
            }
        }
    }

    public void testFragmentSubsetLoadsOnlyTheMissingFragments() throws Exception {
        ColumnStore store = cache.columnStore();
        try (Lease lease = acquire()) {
            Snapshot snapshot = lease.snapshot();
            try (LanceDirectoryReader partial = openCached(snapshot, List.of(0, 1))) {
                readByAdvanceExact(leavesOf(partial).get(0), "rating");
                assertEquals(2, store.entryCount());
                assertTrue(store.contains(snapshot.key(), "rating", 0));
                assertTrue(store.contains(snapshot.key(), "rating", 1));
                assertFalse(store.contains(snapshot.key(), "rating", 2));
            }
            try (LanceDirectoryReader full = openCached(snapshot, allFragments)) {
                List<LanceFragmentLeafReader> leaves = leavesOf(full);
                Long[] values = readByAdvanceExact(leaves.get(2), "rating");
                assertEquals(rating(2 * ROWS), values[0]);
                assertEquals("the second reader scanned fragment 2 only", 2L, store.loadCount());
                assertEquals(3, store.entryCount());
                for (LanceFragmentLeafReader leaf : leaves) {
                    assertTrue(leaf.isServingOffHeap("rating"));
                }
            }
        }
    }

    public void testBudgetFallbackLoadsIntoHeapForTheRequest() throws Exception {
        cache.close();
        // Room for nothing: the smallest column of one fragment does not fit.
        cache = new LanceWarmCache(allocator, 64L, 64, true);
        ColumnStore store = cache.columnStore();
        try (Lease lease = acquire()) {
            Snapshot snapshot = lease.snapshot();
            try (
                LanceDirectoryReader reader = openCached(snapshot, allFragments);
                LanceDirectoryReader heap = openHeap(snapshot, allFragments)
            ) {
                List<LanceFragmentLeafReader> leaves = leavesOf(reader);
                List<LanceFragmentLeafReader> heapLeaves = leavesOf(heap);
                for (int f = 0; f < FRAGMENTS; f++) {
                    Long[] expected = readByAdvanceExact(heapLeaves.get(f), "rating");
                    assertArrayEquals(expected, readByAdvanceExact(leaves.get(f), "rating"));
                    assertTrue(leaves.get(f).isColumnFullyLoaded("rating"));
                    assertFalse("served from heap for this request", leaves.get(f).isServingOffHeap("rating"));
                }
                assertEquals(1L, store.budgetMissCount());
                assertEquals(0L, store.loadCount());
                assertEquals(0, store.entryCount());
                assertEquals(0L, store.allocatedBytes());
            }
        }
    }

    public void testEvictionSkipsPinnedColumnsAndReleasesIdleOnes() throws Exception {
        cache.close();
        // Exactly one numeric column of three fragments fits.
        long oneColumn = FRAGMENTS * ColumnStore.estimateBytes(ROWS, false);
        cache = new LanceWarmCache(allocator, oneColumn, 64, true);
        ColumnStore store = cache.columnStore();
        try (Lease lease = acquire()) {
            Snapshot snapshot = lease.snapshot();
            try (LanceDirectoryReader pinning = openCached(snapshot, allFragments)) {
                List<LanceFragmentLeafReader> leaves = leavesOf(pinning);
                readByAdvanceExact(leaves.get(0), "rating");
                assertEquals(3, store.entryCount());
                assertEquals(oneColumn, store.allocatedBytes());

                // A second column while the first is pinned by this
                // reader: nothing can be evicted, so it loads into heap.
                Long[] ids = readByAdvanceExact(leaves.get(0), "id");
                assertEquals(Long.valueOf(0L), ids[0]);
                assertFalse(leaves.get(0).isServingOffHeap("id"));
                assertTrue(leaves.get(0).isColumnFullyLoaded("id"));
                assertEquals(1L, store.budgetMissCount());
                assertEquals(0L, store.evictionCount());
                assertTrue(store.contains(snapshot.key(), "rating", 0));
            }
            // The reader closed and unpinned rating; the next column
            // evicts it.
            try (LanceDirectoryReader second = openCached(snapshot, allFragments)) {
                List<LanceFragmentLeafReader> leaves = leavesOf(second);
                Long[] ids = readByAdvanceExact(leaves.get(1), "id");
                assertEquals(Long.valueOf(ROWS), ids[0]);
                assertTrue(leaves.get(1).isServingOffHeap("id"));
                assertEquals(3L, store.evictionCount());
                assertFalse(store.contains(snapshot.key(), "rating", 0));
                assertTrue(store.contains(snapshot.key(), "id", 0));
                assertEquals(3, store.entryCount());
                assertEquals(oneColumn, store.allocatedBytes());
            }
        }
    }

    public void testDeletedRowsAreMaskedInBothPaths() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            // Rows 205 and 210 sit in fragment 1; 205 has a rating, 210 too.
            dataset.delete("id = 205 OR id = 210");
        }
        try (Lease lease = acquire()) {
            Snapshot snapshot = lease.snapshot();
            assertTrue(snapshot.fragment(1).hasDeletionFile());
            try (
                LanceDirectoryReader offHeap = openCached(snapshot, allFragments);
                LanceDirectoryReader heap = openHeap(snapshot, allFragments)
            ) {
                LanceFragmentLeafReader offHeapLeaf = leavesOf(offHeap).get(1);
                LanceFragmentLeafReader heapLeaf = leavesOf(heap).get(1);
                assertEquals(ROWS - 2, offHeapLeaf.numDocs());
                assertNotNull(offHeapLeaf.getLiveDocs());
                assertFalse(offHeapLeaf.getLiveDocs().get(5));
                assertFalse(offHeapLeaf.getLiveDocs().get(10));
                assertSameColumn("rating", heapLeaf, offHeapLeaf);
                assertSameColumn("flag", heapLeaf, offHeapLeaf);
                Long[] ratings = readByAdvanceExact(offHeapLeaf, "rating");
                assertNull("deleted row reports no value", ratings[5]);
                assertNull(ratings[10]);
                assertEquals(rating(206), ratings[6]);
                // The store's validity bit is clear for the deleted rows as well.
                CachedColumn column = offHeapLeaf.offHeapColumn("rating");
                assertFalse(column.isSet(5));
                assertTrue(column.isSet(6));
            }
        }
    }

    public void testSparseHintStillTakesRowsAndLeavesTheStoreAlone() throws Exception {
        ColumnStore store = cache.columnStore();
        try (Lease lease = acquire()) {
            try (LanceDirectoryReader reader = openCached(lease.snapshot(), allFragments)) {
                LanceFragmentLeafReader leaf = leavesOf(reader).get(1);
                int[] hint = { 3, 17, 44 };
                leaf.hintMatchedOffsets(hint, false);
                NumericDocValues rating = leaf.getNumericDocValues("rating");
                assertTrue(rating.advanceExact(3));
                assertEquals(rating(ROWS + 3).longValue(), rating.longValue());
                assertTrue(leaf.isServingSparse("rating"));
                assertFalse(leaf.isServingOffHeap("rating"));
                assertEquals("the hinted take does not populate the store", 0L, store.loadCount());
                assertEquals(0, store.entryCount());

                // A doc outside the hint switches this leaf to the full
                // column. The store serves that fragment alone; the other
                // leaves keep their hinted rows.
                assertTrue(rating.advanceExact(5));
                assertEquals(rating(ROWS + 5).longValue(), rating.longValue());
                assertTrue(leaf.isServingOffHeap("rating"));
                assertEquals(1L, store.loadCount());
                assertEquals(1, store.entryCount());
                assertTrue(store.contains(lease.snapshot().key(), "rating", 1));
                assertFalse(leavesOf(reader).get(0).isServingOffHeap("rating"));
                assertFalse(leavesOf(reader).get(0).isColumnFullyLoaded("rating"));
            }
        }
    }

    public void testBreakerAccountsTheColumnCacheAndTripsALoad() throws Exception {
        RecordingBreaker breaker = new RecordingBreaker(1L << 40);
        LanceCircuitBreaker.setBreaker(breaker);
        try (Lease lease = acquire()) {
            try (LanceDirectoryReader reader = openCached(lease.snapshot(), allFragments)) {
                readByAdvanceExact(leavesOf(reader).get(0), "rating");
            }
        }
        long columnBytes = cache.columnCacheBytes();
        assertTrue(columnBytes > 0L);
        // The plugin's sampler reports Session bytes plus column cache
        // bytes; with no Session in this test the estimate is the column
        // cache alone.
        LanceCircuitBreaker.updateUsage(0L, columnBytes);
        assertEquals("the breaker estimate grew by the column cache", columnBytes, breaker.getUsed());

        // At the limit, a load that would be needed is refused with the
        // column cache label; an already loaded column is still served.
        LanceCircuitBreaker.setBreaker(new RecordingBreaker(1L));
        LanceCircuitBreaker.updateUsage(0L, columnBytes);
        try (Lease lease = acquire()) {
            Snapshot snapshot = lease.snapshot();
            try (LanceDirectoryReader reader = openCached(snapshot, allFragments)) {
                LanceFragmentLeafReader leaf = leavesOf(reader).get(0);
                assertEquals(rating(0), readByAdvanceExact(leaf, "rating")[0]);
                CircuitBreakingException tripped = expectThrows(CircuitBreakingException.class, () -> readByAdvanceExact(leaf, "id"));
                assertTrue(tripped.getMessage(), tripped.getMessage().contains(ColumnStore.BREAKER_LABEL));
            }
        }
    }

    /** Minimal breaker that records what {@link LanceCircuitBreaker} reports. */
    private static final class RecordingBreaker implements CircuitBreaker {
        private final AtomicLong used = new AtomicLong();
        private final long limit;

        RecordingBreaker(long limit) {
            this.limit = limit;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {}

        @Override
        public double addEstimateBytesAndMaybeBreak(long bytes, String label) {
            used.addAndGet(bytes);
            return 1.0;
        }

        @Override
        public long addWithoutBreaking(long bytes) {
            return used.addAndGet(bytes);
        }

        @Override
        public long getUsed() {
            return used.get();
        }

        @Override
        public long getLimit() {
            return limit;
        }

        @Override
        public double getOverhead() {
            return 1.0;
        }

        @Override
        public long getTrippedCount() {
            return 0;
        }

        @Override
        public String getName() {
            return LanceCircuitBreaker.NAME;
        }

        @Override
        public Durability getDurability() {
            return Durability.TRANSIENT;
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {}
    }
}
