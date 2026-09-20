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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.memory.RootAllocator;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.lance.Dataset;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.lance.LanceCircuitBreaker;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.engine.LanceWarmCache.Lease;
import org.opensearch.lance.engine.LanceWarmCache.Snapshot;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Off-heap doc values served from {@link ColumnStore} through
 * {@link LanceDirectoryReader#openForSnapshot}: every numeric value and
 * presence bit equals the heap {@code long[]} path, every keyword
 * dictionary, ordinal and term lookup equals the heap {@code BytesRef[]}
 * path, one scan per (snapshot, column), pins block eviction, the budget
 * fallback loads into heap, deleted rows are masked, and the breaker sees
 * the footprint. The fixture has three fragments of 10,000 rows, so a
 * hint of at most 25 rows ({@link LanceFragmentLeafReader#SPARSE_RATIO})
 * is sparse and a hint of 100 rows (1 percent) is not.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ColumnStoreDocValuesTests extends OpenSearchTestCase {

    private static final int FRAGMENTS = 3;
    private static final int ROWS = 10_000;
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
        return LanceDirectoryReader.openForSnapshot(
            new ByteBuffersDirectory(),
            snapshot,
            cache.columnStore(),
            fragmentIds,
            null,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST)
        );
    }

    /** Heap reference: the same snapshot without a store, so every column loads into {@code long[]}. */
    private LanceDirectoryReader openHeap(Snapshot snapshot, List<Integer> fragmentIds) throws IOException {
        return LanceDirectoryReader.openForSnapshot(
            new ByteBuffersDirectory(),
            snapshot,
            null,
            fragmentIds,
            null,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST)
        );
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
            // Rows 10,005 and 10,010 sit in fragment 1; both have a rating.
            dataset.delete("id = " + (ROWS + 5) + " OR id = " + (ROWS + 10));
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
                assertEquals(rating(ROWS + 6), ratings[6]);
                // The store's validity bit is clear for the deleted rows as well.
                CachedColumn column = offHeapLeaf.offHeapColumn("rating");
                assertFalse(column.isSet(5));
                assertTrue(column.isSet(6));
            }
        }
    }

    public void testSparseHintStillTakesRowsAndLeavesTheStoreAlone() throws Exception {
        // Hint below the ratio, store empty: the leaf takes the hinted
        // rows and starts no store load.
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

    /** Twenty ascending offsets, below the sparse ratio on a 10,000 row fragment. */
    private static int[] twentyOffsets() {
        int[] hint = new int[20];
        for (int i = 0; i < hint.length; i++) {
            hint[i] = 3 + i * 7;
        }
        return hint;
    }

    public void testStoreHeldColumnsAreReadInsteadOfTheHintedTake() throws Exception {
        // A first reader loads rating, flag, category and tags into the
        // store for every fragment. A second reader with a 20 row
        // exclusive hint (0.2 percent, sparse) then reads the store
        // instead of taking the rows: no take, no scan, and the keyword
        // dictionaries are the full ones. Only the hinted leaf is
        // published; the leaves the request did not touch stay untouched.
        ColumnStore store = cache.columnStore();
        try (Lease lease = acquire()) {
            try (LanceDirectoryReader first = openCached(lease.snapshot(), allFragments)) {
                LanceFragmentLeafReader leaf = leavesOf(first).get(0);
                readByAdvanceExact(leaf, "rating");
                readByAdvanceExact(leaf, "flag");
                ordsByDoc(leaf.getSortedDocValues("category"), leaf.maxDoc());
                rowsByDoc(leaf.getSortedSetDocValues("tags"), leaf.maxDoc());
            }
        }
        assertEquals(4L, store.loadCount());
        assertEquals(4 * FRAGMENTS, store.entryCount());
        long hitsBefore = store.hitCount();
        try (Lease lease = acquire()) {
            Snapshot snapshot = lease.snapshot();
            try (LanceDirectoryReader reader = openCached(snapshot, allFragments)) {
                List<LanceFragmentLeafReader> leaves = leavesOf(reader);
                LanceFragmentLeafReader leaf = leaves.get(1);
                int[] hint = twentyOffsets();
                leaf.hintMatchedOffsets(hint, true);

                NumericDocValues rating = leaf.getNumericDocValues("rating");
                for (int offset : hint) {
                    Long expected = rating(ROWS + offset);
                    assertEquals(expected != null, rating.advanceExact(offset));
                    if (expected != null) {
                        assertEquals(expected.longValue(), rating.longValue());
                    }
                }
                assertTrue("the store holds the fragment, so it is read", leaf.isServingOffHeap("rating"));
                assertFalse("no take for a column the store holds", leaf.isServingSparse("rating"));
                assertEquals("a doc outside the hint costs nothing extra", ROWS, rating.cost());
                assertTrue(rating.advanceExact(0));
                assertEquals(rating(ROWS).longValue(), rating.longValue());

                NumericDocValues flag = leaf.getNumericDocValues("flag");
                assertTrue(flag.advanceExact(hint[0]));
                assertTrue(leaf.isServingOffHeap("flag"));
                assertFalse(leaf.isServingSparse("flag"));

                SortedDocValues category = leaf.getSortedDocValues("category");
                assertEquals("the full dictionary, not the one of the hinted rows", 3, category.getValueCount());
                assertTrue(leaf.isServingOffHeap("category"));
                assertFalse(leaf.isServingSparse("category"));
                for (int offset : hint) {
                    String expected = category(ROWS + offset);
                    assertEquals(expected != null, category.advanceExact(offset));
                    if (expected != null) {
                        assertEquals(expected, category.lookupOrd(category.ordValue()).utf8ToString());
                    }
                }
                SortedSetDocValues tags = leaf.getSortedSetDocValues("tags");
                assertEquals(5L, tags.getValueCount());
                assertTrue(leaf.isServingOffHeap("tags"));
                assertFalse(leaf.isServingSparse("tags"));

                assertEquals("nothing was scanned", 4L, store.loadCount());
                assertEquals(4 * FRAGMENTS, store.entryCount());
                assertEquals("one store hit per column", hitsBefore + 4, store.hitCount());
                for (int f : new int[] { 0, 2 }) {
                    assertFalse("fragment " + f + " was not touched", leaves.get(f).isColumnFullyLoaded("rating"));
                    assertFalse(leaves.get(f).isColumnFullyLoaded("category"));
                }
            }
        }
    }

    public void testHintAboveTheRatioLoadsTheColumnIntoTheStore() throws Exception {
        // 100 of 10,000 rows (1 percent) is above the ratio: the leaf
        // loads the column through the shard cache, which puts every
        // fragment of the reader into the store in one scan.
        ColumnStore store = cache.columnStore();
        try (Lease lease = acquire()) {
            Snapshot snapshot = lease.snapshot();
            try (LanceDirectoryReader reader = openCached(snapshot, allFragments)) {
                List<LanceFragmentLeafReader> leaves = leavesOf(reader);
                LanceFragmentLeafReader leaf = leaves.get(1);
                int[] hint = new int[100];
                for (int i = 0; i < hint.length; i++) {
                    hint[i] = i * 3;
                }
                leaf.hintMatchedOffsets(hint, true);
                NumericDocValues rating = leaf.getNumericDocValues("rating");
                assertTrue(rating.advanceExact(3));
                assertEquals(rating(ROWS + 3).longValue(), rating.longValue());
                assertFalse(leaf.isServingSparse("rating"));
                assertTrue(leaf.isServingOffHeap("rating"));
                assertEquals(1L, store.loadCount());
                assertEquals(FRAGMENTS, store.entryCount());
                for (LanceFragmentLeafReader other : leaves) {
                    assertTrue(store.contains(snapshot.key(), "rating", other.fragmentId()));
                    assertTrue(other.isServingOffHeap("rating"));
                }

                SortedDocValues category = leaf.getSortedDocValues("category");
                assertEquals(3, category.getValueCount());
                assertFalse(leaf.isServingSparse("category"));
                assertTrue(leaf.isServingOffHeap("category"));
                assertEquals(2L, store.loadCount());
                assertEquals(2 * FRAGMENTS, store.entryCount());
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

    // Keyword columns: dictionary and ordinals in the store.

    private static String category(int i) {
        return i % 4 == 3 ? null : "c" + (i % 3);
    }

    private static List<String> tags(int i) {
        if (i % 6 == 5) {
            return null;
        }
        return new ArrayList<>(new TreeSet<>(List.of("t" + (i % 2), "t" + (i % 5))));
    }

    /** Every term of {@code values} in ordinal order, copied out of whatever scratch the instance returns. */
    private static List<String> dictionaryOf(SortedDocValues values) throws IOException {
        List<String> terms = new ArrayList<>();
        for (int ord = 0; ord < values.getValueCount(); ord++) {
            terms.add(values.lookupOrd(ord).utf8ToString());
        }
        return terms;
    }

    private static List<String> dictionaryOf(SortedSetDocValues values) throws IOException {
        List<String> terms = new ArrayList<>();
        for (long ord = 0; ord < values.getValueCount(); ord++) {
            terms.add(values.lookupOrd(ord).utf8ToString());
        }
        return terms;
    }

    /** Ordinal of every doc through advanceExact, {@code -1} where the doc has no value. */
    private static int[] ordsByDoc(SortedDocValues values, int maxDoc) throws IOException {
        int[] ords = new int[maxDoc];
        for (int doc = 0; doc < maxDoc; doc++) {
            ords[doc] = values.advanceExact(doc) ? values.ordValue() : -1;
        }
        return ords;
    }

    /** Ordinals of every doc through advanceExact / nextOrd, an empty array where the doc has no value. */
    private static List<int[]> rowsByDoc(SortedSetDocValues values, int maxDoc) throws IOException {
        List<int[]> rows = new ArrayList<>();
        for (int doc = 0; doc < maxDoc; doc++) {
            if (!values.advanceExact(doc)) {
                rows.add(new int[0]);
                continue;
            }
            int[] row = new int[values.docValueCount()];
            for (int i = 0; i < row.length; i++) {
                row[i] = (int) values.nextOrd();
            }
            rows.add(row);
        }
        return rows;
    }

    /** Doc ids yielded by nextDoc iteration. */
    private static List<Integer> iterate(DocIdSetIterator values) throws IOException {
        List<Integer> docs = new ArrayList<>();
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            docs.add(doc);
        }
        return docs;
    }

    private static void assertSortedUnique(List<String> terms) {
        for (int i = 1; i < terms.size(); i++) {
            assertTrue(
                "dictionary must ascend in unsigned byte order: " + terms,
                new BytesRef(terms.get(i - 1)).compareTo(new BytesRef(terms.get(i))) < 0
            );
        }
    }

    private static final String[] ABSENT_TERMS = { "", "b", "c", "c0a", "c1\u0000", "c9", "d", "t", "t5", "zzz", "\u00ff" };

    /** The store and heap instances of the single-valued keyword {@code column} agree on every read. */
    private static void assertSameKeyword(String column, LanceFragmentLeafReader heap, LanceFragmentLeafReader offHeap) throws IOException {
        SortedDocValues expected = heap.getSortedDocValues(column);
        SortedDocValues actual = offHeap.getSortedDocValues(column);
        assertEquals(column + " value count on fragment " + heap.fragmentId(), expected.getValueCount(), actual.getValueCount());
        List<String> expectedTerms = dictionaryOf(expected);
        List<String> actualTerms = dictionaryOf(actual);
        assertEquals(column + " dictionary on fragment " + heap.fragmentId(), expectedTerms, actualTerms);
        assertSortedUnique(actualTerms);
        assertArrayEquals(
            column + " ordinals on fragment " + heap.fragmentId(),
            ordsByDoc(expected, heap.maxDoc()),
            ordsByDoc(actual, offHeap.maxDoc())
        );
        assertEquals(column + " iteration on fragment " + heap.fragmentId(), iterate(expected), iterate(actual));
        for (String term : actualTerms) {
            BytesRef key = new BytesRef(term);
            assertEquals("lookupTerm(" + term + ")", expected.lookupTerm(key), actual.lookupTerm(key));
            assertEquals(term, actual.lookupOrd(actual.lookupTerm(key)).utf8ToString());
        }
        for (String absent : ABSENT_TERMS) {
            BytesRef key = new BytesRef(absent);
            int found = actual.lookupTerm(key);
            assertTrue("absent term " + absent + " must report an insertion point", found < 0);
            assertEquals("lookupTerm(absent " + absent + ")", expected.lookupTerm(key), found);
        }
        // Lucene's termsEnum walks the dictionary through lookupOrd.
        List<String> viaTermsEnum = new ArrayList<>();
        TermsEnum termsEnum = actual.termsEnum();
        for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
            viaTermsEnum.add(term.utf8ToString());
        }
        assertEquals(actualTerms, viaTermsEnum);
    }

    /** The store and heap instances of the multi-valued keyword {@code column} agree on every read. */
    private static void assertSameKeywordArray(String column, LanceFragmentLeafReader heap, LanceFragmentLeafReader offHeap)
        throws IOException {
        SortedSetDocValues expected = heap.getSortedSetDocValues(column);
        SortedSetDocValues actual = offHeap.getSortedSetDocValues(column);
        assertEquals(column + " value count on fragment " + heap.fragmentId(), expected.getValueCount(), actual.getValueCount());
        List<String> expectedTerms = dictionaryOf(expected);
        List<String> actualTerms = dictionaryOf(actual);
        assertEquals(column + " dictionary on fragment " + heap.fragmentId(), expectedTerms, actualTerms);
        assertSortedUnique(actualTerms);
        List<int[]> expectedRows = rowsByDoc(expected, heap.maxDoc());
        List<int[]> actualRows = rowsByDoc(actual, offHeap.maxDoc());
        assertEquals(expectedRows.size(), actualRows.size());
        for (int doc = 0; doc < expectedRows.size(); doc++) {
            assertArrayEquals(
                column + " ordinals of doc " + doc + " on fragment " + heap.fragmentId(),
                expectedRows.get(doc),
                actualRows.get(doc)
            );
            int[] row = actualRows.get(doc);
            for (int i = 1; i < row.length; i++) {
                assertTrue("row ordinals must strictly ascend", row[i - 1] < row[i]);
            }
        }
        assertEquals(column + " iteration on fragment " + heap.fragmentId(), iterate(expected), iterate(actual));
        for (String term : actualTerms) {
            BytesRef key = new BytesRef(term);
            assertEquals("lookupTerm(" + term + ")", expected.lookupTerm(key), actual.lookupTerm(key));
        }
        for (String absent : ABSENT_TERMS) {
            BytesRef key = new BytesRef(absent);
            assertEquals("lookupTerm(absent " + absent + ")", expected.lookupTerm(key), actual.lookupTerm(key));
        }
    }

    public void testKeywordDictionaryAndOrdinalsEqualTheHeapPath() throws Exception {
        ColumnStore store = cache.columnStore();
        try (Lease lease = acquire()) {
            Snapshot snapshot = lease.snapshot();
            try (
                LanceDirectoryReader offHeap = openCached(snapshot, allFragments);
                LanceDirectoryReader heap = openHeap(snapshot, allFragments)
            ) {
                List<LanceFragmentLeafReader> offHeapLeaves = leavesOf(offHeap);
                List<LanceFragmentLeafReader> heapLeaves = leavesOf(heap);
                BytesRefBuilder scratch = new BytesRefBuilder();
                long entryBytes = 0L;
                for (int f = 0; f < FRAGMENTS; f++) {
                    LanceFragmentLeafReader leaf = offHeapLeaves.get(f);
                    assertSameKeyword("category", heapLeaves.get(f), leaf);
                    assertSameKeywordArray("tags", heapLeaves.get(f), leaf);
                    assertTrue(leaf.isServingOffHeap("category"));
                    assertTrue(leaf.isServingOffHeap("tags"));
                    assertTrue(heapLeaves.get(f).isColumnFullyLoaded("category"));
                    assertFalse("the reference reader has no store", heapLeaves.get(f).isServingOffHeap("category"));
                    assertFalse(heapLeaves.get(f).isServingOffHeap("tags"));

                    // Against the fixture formulas: c0 < c1 < c2, null every fourth row.
                    CachedKeywordColumn category = leaf.offHeapKeywordColumn("category");
                    assertEquals(3, category.valueCount());
                    assertEquals(ROWS, category.rows());
                    for (int offset = 0; offset < ROWS; offset++) {
                        int i = f * ROWS + offset;
                        String expected = category(i);
                        if (expected == null) {
                            assertEquals("row " + i + " is null", -1, category.ord(offset));
                        } else {
                            assertEquals("row " + i, expected, category.term(category.ord(offset), scratch).utf8ToString());
                            assertEquals(i % 3, category.ord(offset));
                        }
                    }
                    assertEquals(1, category.lookupTerm(new BytesRef("c1")));
                    assertEquals(-1, category.lookupTerm(new BytesRef("a")));
                    assertEquals(-4, category.lookupTerm(new BytesRef("c3")));

                    // Offsets are contiguous; a null list is an empty range; rows are sorted and unique.
                    CachedKeywordArrayColumn tags = leaf.offHeapKeywordArrayColumn("tags");
                    assertEquals(5, tags.valueCount());
                    assertEquals(0, tags.rowStart(0));
                    for (int offset = 0; offset < ROWS; offset++) {
                        int i = f * ROWS + offset;
                        int start = tags.rowStart(offset);
                        int end = tags.rowEnd(offset);
                        assertTrue(start <= end);
                        if (offset + 1 < ROWS) {
                            assertEquals(end, tags.rowStart(offset + 1));
                        }
                        List<String> expected = tags(i);
                        List<String> actual = new ArrayList<>();
                        for (int k = start; k < end; k++) {
                            actual.add(tags.term(tags.ordinal(k), scratch).utf8ToString());
                        }
                        assertEquals("tags of row " + i, expected == null ? List.of() : expected, actual);
                    }
                    entryBytes += category.bytes() + tags.bytes();
                }
                assertEquals("one scan per keyword column", 2L, store.loadCount());
                assertEquals(2 * FRAGMENTS, store.entryCount());
                assertTrue(entryBytes > 0L);
                assertTrue(
                    "entry bytes are the buffer capacities, at most what the allocator accounts",
                    entryBytes <= store.allocatedBytes()
                );
                assertEquals(store.allocatedBytes(), cache.columnCacheBytes());
            }
        }
    }

    public void testKeywordSubFieldOfAnFtsColumnIsServedFromTheStore() throws Exception {
        LinkedHashMap<String, String> raw = new LinkedHashMap<>();
        raw.put("raw", "keyword");
        Map<String, LinkedHashMap<String, String>> multiFields = Map.of("body", raw);
        ColumnStore store = cache.columnStore();
        try (Lease lease = cache.acquire(UUID, uri, StorageOptions.empty(), Optional.empty(), "", LancePrimaryKeyType.NONE, multiFields)) {
            Snapshot snapshot = lease.snapshot();
            try (
                LanceDirectoryReader offHeap = openCached(snapshot, allFragments);
                LanceDirectoryReader heap = openHeap(snapshot, allFragments)
            ) {
                for (int f = 0; f < FRAGMENTS; f++) {
                    LanceFragmentLeafReader leaf = leavesOf(offHeap).get(f);
                    assertSameKeyword("body.raw", leavesOf(heap).get(f), leaf);
                    // Every body is distinct, so the dictionary has one term per row.
                    assertEquals(ROWS, leaf.getSortedDocValues("body.raw").getValueCount());
                    assertTrue("the sub field resolves to the base column's store entry", leaf.isServingOffHeap("body"));
                    assertTrue(store.contains(snapshot.key(), "body", f));
                }
                assertEquals(1L, store.loadCount());
            }
        }
    }

    public void testSecondReaderReadsTheKeywordStoreWithoutAScan() throws Exception {
        ColumnStore store = cache.columnStore();
        try (Lease lease = acquire()) {
            try (LanceDirectoryReader first = openCached(lease.snapshot(), allFragments)) {
                LanceFragmentLeafReader leaf = leavesOf(first).get(0);
                ordsByDoc(leaf.getSortedDocValues("category"), leaf.maxDoc());
                rowsByDoc(leaf.getSortedSetDocValues("tags"), leaf.maxDoc());
                assertEquals(2L, store.loadCount());
                assertEquals(0L, store.hitCount());
            }
        }
        long bytesAfterFirst = store.allocatedBytes();
        try (Lease lease = acquire()) {
            try (LanceDirectoryReader second = openCached(lease.snapshot(), allFragments)) {
                for (LanceFragmentLeafReader leaf : leavesOf(second)) {
                    SortedDocValues category = leaf.getSortedDocValues("category");
                    for (int offset = 0; offset < ROWS; offset++) {
                        int i = leaf.fragmentId() * ROWS + offset;
                        assertEquals(category(i) != null, category.advanceExact(offset));
                        if (category(i) != null) {
                            assertEquals(category(i), category.lookupOrd(category.ordValue()).utf8ToString());
                        }
                    }
                    SortedSetDocValues tags = leaf.getSortedSetDocValues("tags");
                    // Offset 2 of every fragment (rows 2, 10,002, 20,002)
                    // carries t0 and t2; offset 0 carries t0 twice.
                    assertTrue(tags.advanceExact(2));
                    assertEquals(2, tags.docValueCount());
                    assertTrue(tags.advanceExact(0));
                    assertEquals(1, tags.docValueCount());
                    assertTrue(leaf.isServingOffHeap("category"));
                    assertTrue(leaf.isServingOffHeap("tags"));
                }
                assertEquals("no second scan", 2L, store.loadCount());
                assertEquals(2L, store.hitCount());
                assertEquals("no new allocation", bytesAfterFirst, store.allocatedBytes());
            }
        }
    }

    public void testKeywordBudgetFallbackBuildsTheHeapDictionaryForTheRequest() throws Exception {
        cache.close();
        // Room for something, but not for a dictionary: the budget is
        // positive so the keyword loaders take the store path, scan, and
        // only then find that nothing fits.
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
                    assertSameKeyword("category", heapLeaves.get(f), leaves.get(f));
                    assertSameKeywordArray("tags", heapLeaves.get(f), leaves.get(f));
                    assertFalse("served from heap for this request", leaves.get(f).isServingOffHeap("category"));
                    assertFalse(leaves.get(f).isServingOffHeap("tags"));
                    assertTrue(leaves.get(f).isColumnFullyLoaded("category"));
                    assertTrue(leaves.get(f).isColumnFullyLoaded("tags"));
                }
                assertEquals("one miss per keyword column", 2L, store.budgetMissCount());
                assertEquals(0L, store.loadCount());
                assertEquals(0, store.entryCount());
                assertEquals(0L, store.allocatedBytes());
                // The scan that found the dictionaries too large is the
                // one that served the request: one per column, and the
                // reader's own heap loaders never ran.
                assertEquals("one store scan per keyword column", 2L, store.scanCount());
                assertEquals("no second scan on the heap path", 0L, leaves.get(0).shardColumnCache().heapScanCount());
            }
        }
    }

    public void testZeroKeywordBudgetSkipsTheStoreAndScansOnce() throws Exception {
        cache.close();
        // No budget at all: the outcome of a keyword load is known before
        // its scan, so the reader takes the heap path directly.
        cache = new LanceWarmCache(allocator, 0L, 64, true);
        ColumnStore store = cache.columnStore();
        assertEquals(0L, store.limitBytes());
        try (Lease lease = acquire()) {
            Snapshot snapshot = lease.snapshot();
            try (
                LanceDirectoryReader reader = openCached(snapshot, allFragments);
                LanceDirectoryReader heap = openHeap(snapshot, allFragments)
            ) {
                List<LanceFragmentLeafReader> leaves = leavesOf(reader);
                List<LanceFragmentLeafReader> heapLeaves = leavesOf(heap);
                for (int f = 0; f < FRAGMENTS; f++) {
                    assertSameKeyword("category", heapLeaves.get(f), leaves.get(f));
                    assertSameKeywordArray("tags", heapLeaves.get(f), leaves.get(f));
                    assertFalse(leaves.get(f).isServingOffHeap("category"));
                    assertFalse(leaves.get(f).isServingOffHeap("tags"));
                    assertTrue(leaves.get(f).isColumnFullyLoaded("category"));
                    assertTrue(leaves.get(f).isColumnFullyLoaded("tags"));
                }
                assertEquals("the store was never asked", 0L, store.scanCount());
                assertEquals(0L, store.budgetMissCount());
                assertEquals(0L, store.loadCount());
                assertEquals(0, store.entryCount());
                assertEquals("one heap scan per keyword column", 2L, leaves.get(0).shardColumnCache().heapScanCount());
                // Numeric columns keep their pre-scan budget check: one
                // miss, and the heap loader scans once.
                readByAdvanceExact(leaves.get(0), "rating");
                assertEquals(1L, store.budgetMissCount());
                assertEquals(0L, store.scanCount());
                assertEquals(3L, leaves.get(0).shardColumnCache().heapScanCount());
            }
        }
    }

    public void testKeywordThatFitsTheBudgetStillEntersTheStore() throws Exception {
        ColumnStore store = cache.columnStore();
        try (Lease lease = acquire()) {
            Snapshot snapshot = lease.snapshot();
            try (LanceDirectoryReader reader = openCached(snapshot, allFragments)) {
                List<LanceFragmentLeafReader> leaves = leavesOf(reader);
                for (LanceFragmentLeafReader leaf : leaves) {
                    ordsByDoc(leaf.getSortedDocValues("category"), leaf.maxDoc());
                    rowsByDoc(leaf.getSortedSetDocValues("tags"), leaf.maxDoc());
                    assertTrue(leaf.isServingOffHeap("category"));
                    assertTrue(leaf.isServingOffHeap("tags"));
                    assertTrue(store.contains(snapshot.key(), "category", leaf.fragmentId()));
                    assertTrue(store.contains(snapshot.key(), "tags", leaf.fragmentId()));
                }
                assertEquals(2L, store.loadCount());
                assertEquals(2L, store.scanCount());
                assertEquals(0L, store.budgetMissCount());
                assertEquals(2 * FRAGMENTS, store.entryCount());
                assertEquals(0L, leaves.get(0).shardColumnCache().heapScanCount());
            }
        }
    }

    public void testKeywordBudgetFallbackKeepsTheFragmentsTheStoreAlreadyHeld() throws Exception {
        ColumnStore store = cache.columnStore();
        try (Lease lease = acquire()) {
            Snapshot snapshot = lease.snapshot();
            // Fragment 0's dictionary enters the store while there is room.
            try (LanceDirectoryReader partial = openCached(snapshot, List.of(0))) {
                LanceFragmentLeafReader leaf = leavesOf(partial).get(0);
                ordsByDoc(leaf.getSortedDocValues("category"), leaf.maxDoc());
                assertTrue(store.contains(snapshot.key(), "category", 0));
            }
            long heldBytes = store.allocatedBytes();
            assertTrue(heldBytes > 0L);
            // A store with room for one fragment's dictionary and a half:
            // fragment 0 enters, fragments 1 and 2 together do not fit
            // beside it, are scanned once, and are served from heap while
            // fragment 0 keeps reading the store.
            ColumnStore full = new ColumnStore(allocator, heldBytes + heldBytes / 2);
            try {
                try (
                    LanceDirectoryReader pinning = LanceDirectoryReader.openForSnapshot(
                        new ByteBuffersDirectory(),
                        snapshot,
                        full,
                        List.of(0),
                        null,
                        new NoopCircuitBreaker(CircuitBreaker.REQUEST)
                    )
                ) {
                    LanceFragmentLeafReader leaf = leavesOf(pinning).get(0);
                    ordsByDoc(leaf.getSortedDocValues("category"), leaf.maxDoc());
                    assertTrue(leaf.isServingOffHeap("category"));
                    assertEquals(1L, full.loadCount());
                    try (
                        LanceDirectoryReader all = LanceDirectoryReader.openForSnapshot(
                            new ByteBuffersDirectory(),
                            snapshot,
                            full,
                            allFragments,
                            null,
                            new NoopCircuitBreaker(CircuitBreaker.REQUEST)
                        );
                        LanceDirectoryReader heap = openHeap(snapshot, allFragments)
                    ) {
                        List<LanceFragmentLeafReader> leaves = leavesOf(all);
                        List<LanceFragmentLeafReader> heapLeaves = leavesOf(heap);
                        for (int f = 0; f < FRAGMENTS; f++) {
                            assertSameKeyword("category", heapLeaves.get(f), leaves.get(f));
                        }
                        assertTrue("fragment 0 reads the held entry", leaves.get(0).isServingOffHeap("category"));
                        assertFalse("fragment 1 came back from the scan as heap", leaves.get(1).isServingOffHeap("category"));
                        assertFalse(leaves.get(2).isServingOffHeap("category"));
                        assertEquals("the pinned entry could not be evicted", 0L, full.evictionCount());
                        assertEquals(1L, full.budgetMissCount());
                        assertEquals("one scan for the two missing fragments", 2L, full.scanCount());
                        assertEquals(1L, full.loadCount());
                        assertEquals(0L, leaves.get(0).shardColumnCache().heapScanCount());
                    }
                }
            } finally {
                full.close();
            }
        }
    }

    public void testKeywordEntriesAreEvictedWhenIdleAndReleasedOnClose() throws Exception {
        ColumnStore store = cache.columnStore();
        long oneNumericColumn = FRAGMENTS * ColumnStore.estimateBytes(ROWS, false);
        cache.close();
        // Room for exactly one numeric column of three fragments; the
        // three-term category dictionaries are far smaller.
        cache = new LanceWarmCache(allocator, oneNumericColumn, 64, true);
        store = cache.columnStore();
        try (Lease lease = acquire()) {
            Snapshot snapshot = lease.snapshot();
            long categoryBytes;
            try (LanceDirectoryReader pinning = openCached(snapshot, allFragments)) {
                LanceFragmentLeafReader leaf = leavesOf(pinning).get(0);
                ordsByDoc(leaf.getSortedDocValues("category"), leaf.maxDoc());
                assertEquals(3, store.entryCount());
                categoryBytes = store.allocatedBytes();
                assertTrue(categoryBytes > 0L);
                assertTrue("category must be smaller than the numeric column for the eviction below", categoryBytes < oneNumericColumn);

                // The numeric column does not fit beside the pinned
                // dictionaries and nothing can be evicted: heap for this
                // request, the dictionaries stay.
                Long[] ratings = readByAdvanceExact(leaf, "rating");
                assertEquals(rating(0), ratings[0]);
                assertFalse(leaf.isServingOffHeap("rating"));
                assertEquals(1L, store.budgetMissCount());
                assertEquals(0L, store.evictionCount());
                assertTrue(store.contains(snapshot.key(), "category", 0));
            }
            // Unpinned now: the numeric column evicts all three dictionaries.
            try (LanceDirectoryReader second = openCached(snapshot, allFragments)) {
                LanceFragmentLeafReader leaf = leavesOf(second).get(1);
                assertEquals(rating(ROWS), readByAdvanceExact(leaf, "rating")[0]);
                assertTrue(leaf.isServingOffHeap("rating"));
                assertEquals(3L, store.evictionCount());
                assertFalse(store.contains(snapshot.key(), "category", 0));
                assertTrue(store.contains(snapshot.key(), "rating", 1));
                assertEquals(3, store.entryCount());
                assertEquals("the dictionaries' bytes went back to the allocator", oneNumericColumn, store.allocatedBytes());
            }
        }
        // A fresh store, filled with keyword entries, releases them on close.
        cache.close();
        cache = new LanceWarmCache(allocator, 64L * 1024 * 1024, 64, true);
        store = cache.columnStore();
        try (Lease lease = acquire()) {
            try (LanceDirectoryReader reader = openCached(lease.snapshot(), allFragments)) {
                LanceFragmentLeafReader leaf = leavesOf(reader).get(0);
                ordsByDoc(leaf.getSortedDocValues("category"), leaf.maxDoc());
                rowsByDoc(leaf.getSortedSetDocValues("tags"), leaf.maxDoc());
                assertEquals(6, store.entryCount());
                assertTrue(store.allocatedBytes() > 0L);
            }
        }
        cache.close();
        assertEquals(0, store.entryCount());
        assertEquals(0L, store.allocatedBytes());
        cache = null;
    }

    public void testDeletedRowsHaveNoKeywordValueInBothPaths() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            // Rows 10,005 (c0, tags t0/t1) and 10,010 (c2, tags t0) sit in
            // fragment 1 and both carry values.
            assertEquals("c0", category(ROWS + 5));
            assertEquals("c2", category(ROWS + 10));
            dataset.delete("id = " + (ROWS + 5) + " OR id = " + (ROWS + 10));
        }
        try (Lease lease = acquire()) {
            Snapshot snapshot = lease.snapshot();
            try (
                LanceDirectoryReader offHeap = openCached(snapshot, allFragments);
                LanceDirectoryReader heap = openHeap(snapshot, allFragments)
            ) {
                LanceFragmentLeafReader offHeapLeaf = leavesOf(offHeap).get(1);
                LanceFragmentLeafReader heapLeaf = leavesOf(heap).get(1);
                assertEquals(ROWS - 2, offHeapLeaf.numDocs());
                assertSameKeyword("category", heapLeaf, offHeapLeaf);
                assertSameKeywordArray("tags", heapLeaf, offHeapLeaf);
                SortedDocValues category = offHeapLeaf.getSortedDocValues("category");
                assertFalse("deleted row reports no value", category.advanceExact(5));
                assertFalse(category.advanceExact(10));
                assertTrue(category.advanceExact(6));
                assertEquals(category(ROWS + 6), category.lookupOrd(category.ordValue()).utf8ToString());
                assertEquals(-1, offHeapLeaf.offHeapKeywordColumn("category").ord(5));
                CachedKeywordArrayColumn tags = offHeapLeaf.offHeapKeywordArrayColumn("tags");
                assertEquals("deleted row has an empty ordinal range", tags.rowStart(5), tags.rowEnd(5));
                assertEquals(tags.rowStart(10), tags.rowEnd(10));
                // The term still exists on the fragment through other rows.
                assertEquals(3, offHeapLeaf.offHeapKeywordColumn("category").valueCount());
            }
        }
    }

    public void testExclusiveHintKeepsKeywordSparseAndFallsBackToTheStoreForOneFragment() throws Exception {
        ColumnStore store = cache.columnStore();
        try (Lease lease = acquire()) {
            Snapshot snapshot = lease.snapshot();
            try (LanceDirectoryReader reader = openCached(snapshot, allFragments)) {
                LanceFragmentLeafReader leaf = leavesOf(reader).get(1);
                // Rows 10,001 (c2), 10,002 (c0), 10,003 (null), 10,008 (c0),
                // 10,010 (c2), 10,012 (c1)
                int[] hint = { 1, 2, 3, 8, 10, 12 };
                leaf.hintMatchedOffsets(hint, true);
                SortedDocValues category = leaf.getSortedDocValues("category");
                assertEquals(3, category.getValueCount());
                assertEquals("c0", category(ROWS + 2));
                assertTrue(category.advanceExact(2));
                assertEquals("c0", category.lookupOrd(category.ordValue()).utf8ToString());
                assertTrue("an exclusive sparse hint takes the rows while the store is empty", leaf.isServingSparse("category"));
                assertFalse(leaf.isServingOffHeap("category"));
                assertFalse(leaf.isColumnFullyLoaded("category"));
                assertEquals(0L, store.loadCount());
                assertEquals(0, store.entryCount());

                // Row 10,004 (c2) is outside the hint: the instance learns
                // its term from the full column, which the store serves for
                // this fragment alone, and answers with the sparse ordinal.
                assertEquals("c2", category(ROWS + 4));
                assertTrue(category.advanceExact(4));
                assertEquals(2, category.ordValue());
                assertEquals("c2", category.lookupOrd(category.ordValue()).utf8ToString());
                assertTrue(leaf.isServingOffHeap("category"));
                assertEquals(1L, store.loadCount());
                assertEquals(1, store.entryCount());
                assertTrue(store.contains(snapshot.key(), "category", 1));
                assertFalse(leavesOf(reader).get(0).isColumnFullyLoaded("category"));
                // Row 10,007 is outside the hint and null.
                assertNull(category(ROWS + 7));
                assertFalse(category.advanceExact(7));
                // The decision sticks: later instances stay sparse.
                assertTrue(leaf.isServingSparse("category"));
                assertEquals(3, leaf.getSortedDocValues("category").getValueCount());

                // Same for the multi-valued column: hinted rows carry
                // t0..t3; row 10,006 (t0, t1) is outside the hint.
                assertEquals(List.of("t0", "t1"), tags(ROWS + 6));
                SortedSetDocValues tags = leaf.getSortedSetDocValues("tags");
                assertEquals(4L, tags.getValueCount());
                assertTrue(leaf.isServingSparse("tags"));
                assertTrue(tags.advanceExact(6));
                assertEquals(2, tags.docValueCount());
                assertEquals(0L, tags.nextOrd());
                assertEquals(1L, tags.nextOrd());
                assertTrue(leaf.isServingOffHeap("tags"));
                assertEquals(2L, store.loadCount());
                assertTrue(store.contains(snapshot.key(), "tags", 1));
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
