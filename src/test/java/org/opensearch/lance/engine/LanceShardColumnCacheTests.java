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
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
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
 * Request breaker accounting of the heap column fallback in
 * {@link LanceShardColumnCache} and the single fragment loads of
 * {@link LanceFragmentLeafReader}: every heap load charges the estimate
 * the cache's sizing helpers give before it allocates, a refused charge
 * surfaces as {@link CircuitBreakingException} with nothing left on the
 * breaker, the two step keyword charge is given back whole, and closing
 * the reader returns every byte. The fixture has three fragments of
 * 10,000 rows; the readers here are opened without a column store, so
 * every column takes the heap path the store's budget misses take.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceShardColumnCacheTests extends OpenSearchTestCase {

    private static final int FRAGMENTS = 3;
    private static final int ROWS = 10_000;
    private static final String UUID = "heap-breaker-uuid";

    private RootAllocator allocator;
    private LanceWarmCache cache;
    private String uri;
    private List<Integer> allFragments;
    private long nodeBytesBefore;
    private long nodeRejectionsBefore;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Path scratchDir = createTempDir();
        uri = LanceTableFactory.writeHintFixtureTable(scratchDir, "breaker-" + getTestName(), FRAGMENTS, ROWS);
        allocator = new RootAllocator(Long.MAX_VALUE);
        cache = new LanceWarmCache(allocator, 64L * 1024 * 1024, 64, true);
        allFragments = new ArrayList<>();
        for (int f = 0; f < FRAGMENTS; f++) {
            allFragments.add(f);
        }
        LanceCircuitBreaker.setBreaker(null);
        nodeBytesBefore = HeapFallbackStats.bytes();
        nodeRejectionsBefore = HeapFallbackStats.rejections();
    }

    @Override
    public void tearDown() throws Exception {
        LanceCircuitBreaker.setBreaker(null);
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

    /** A reader without a store: every column loads into heap and is charged to {@code breaker}. */
    private LanceDirectoryReader openHeap(Snapshot snapshot, List<Integer> fragmentIds, CircuitBreaker breaker) throws IOException {
        return LanceDirectoryReader.openForSnapshot(new ByteBuffersDirectory(), snapshot, null, fragmentIds, null, breaker);
    }

    /** A reader over {@code store}, so a keyword column the store scans but cannot hold arrives as a heap dictionary. */
    private LanceDirectoryReader openStored(Snapshot snapshot, ColumnStore store, CircuitBreaker breaker) throws IOException {
        return LanceDirectoryReader.openForSnapshot(new ByteBuffersDirectory(), snapshot, store, allFragments, null, breaker);
    }

    private static List<LanceFragmentLeafReader> leavesOf(LanceDirectoryReader reader) {
        List<LanceFragmentLeafReader> leaves = new ArrayList<>();
        for (LeafReaderContext ctx : reader.leaves()) {
            leaves.add(LanceFragmentLeafReader.unwrap(ctx.reader()));
        }
        return leaves;
    }

    private static long numericBytesOfAllFragments() {
        return FRAGMENTS * LanceShardColumnCache.numericHeapBytes(ROWS);
    }

    /** Ordinal arrays of every fragment plus a dictionary of the three {@code category} terms (c0, c1, c2: six bytes) per fragment. */
    private static long categoryBytesOfAllFragments() {
        return FRAGMENTS * (LanceShardColumnCache.intArrayBytes(ROWS) + LanceShardColumnCache.termsHeapBytes(3, 6L));
    }

    /**
     * Outer arrays, per row ordinal arrays and the five term dictionary
     * (t0 to t4, ten bytes) of {@code tags} over every fragment. Row
     * {@code i} is an Arrow null when {@code i % 6 == 5}, holds one
     * ordinal when {@code i % 10} is 0 or 1 (both elements equal) and
     * two otherwise.
     */
    private static long tagsBytesOfAllFragments() {
        long bytes = 0L;
        for (int fragment = 0; fragment < FRAGMENTS; fragment++) {
            bytes += LanceShardColumnCache.objectArrayBytes(ROWS) + LanceShardColumnCache.termsHeapBytes(5, 10L);
            for (int slot = 0; slot < ROWS; slot++) {
                int i = fragment * ROWS + slot;
                if (i % 6 == 5) {
                    continue;
                }
                bytes += LanceShardColumnCache.intArrayBytes(i % 10 <= 1 ? 1 : 2);
            }
        }
        return bytes;
    }

    private void assertNodeGaugeDelta(long expectedBytes, long expectedRejections) {
        assertEquals("node wide heap fallback bytes", expectedBytes, HeapFallbackStats.bytes() - nodeBytesBefore);
        assertEquals("node wide heap fallback rejections", expectedRejections, HeapFallbackStats.rejections() - nodeRejectionsBefore);
    }

    public void testNumericHeapLoadChargesTheEstimateAndReleasesOnClose() throws Exception {
        LimitedBreaker breaker = new LimitedBreaker(Long.MAX_VALUE);
        try (Lease lease = acquire()) {
            try (LanceDirectoryReader reader = openHeap(lease.snapshot(), allFragments, breaker)) {
                LanceFragmentLeafReader leaf = leavesOf(reader).get(0);
                NumericDocValues rating = leaf.getNumericDocValues("rating");
                assertTrue(rating.advanceExact(0));
                assertEquals(0L, rating.longValue());
                long expected = numericBytesOfAllFragments();
                assertEquals("one charge for the three fragments", expected, breaker.getUsed());
                assertEquals(expected, leaf.shardColumnCache().heapBytesCharged());
                assertSame(breaker, leaf.requestBreaker());
                assertNodeGaugeDelta(expected, 0L);

                // A second column adds its own estimate; a second read of
                // the first column charges nothing more.
                NumericDocValues flag = leavesOf(reader).get(1).getNumericDocValues("flag");
                assertTrue(flag.advanceExact(0));
                assertEquals(2 * expected, breaker.getUsed());
                assertTrue(leaf.getNumericDocValues("rating").advanceExact(5));
                assertEquals(2 * expected, breaker.getUsed());
            }
            assertEquals("close gives every byte back", 0L, breaker.getUsed());
            assertNodeGaugeDelta(0L, 0L);
        }
    }

    public void testRefusedNumericLoadLeavesNothingChargedAndNamesTheColumn() throws Exception {
        // Room for two fragments of the column, not three.
        long limit = 2 * LanceShardColumnCache.numericHeapBytes(ROWS);
        LimitedBreaker breaker = new LimitedBreaker(limit);
        try (Lease lease = acquire()) {
            try (LanceDirectoryReader reader = openHeap(lease.snapshot(), allFragments, breaker)) {
                LanceFragmentLeafReader leaf = leavesOf(reader).get(0);
                NumericDocValues rating = leaf.getNumericDocValues("rating");
                CircuitBreakingException refused = expectThrows(CircuitBreakingException.class, () -> rating.advanceExact(0));
                long wanted = numericBytesOfAllFragments();
                assertTrue(refused.getMessage(), refused.getMessage().contains(LanceShardColumnCache.HEAP_LABEL_PREFIX + "rating"));
                assertTrue(refused.getMessage(), refused.getMessage().contains("column [rating]"));
                assertTrue(refused.getMessage(), refused.getMessage().contains("[" + wanted + "/"));
                assertTrue(refused.getMessage(), refused.getMessage().contains("limit of [" + limit + "/"));
                assertEquals(wanted, refused.getBytesWanted());
                assertEquals(limit, refused.getByteLimit());
                assertEquals(CircuitBreaker.Durability.TRANSIENT, refused.getDurability());
                assertEquals("the refused estimate is not left on the breaker", 0L, breaker.getUsed());
                assertEquals(0L, leaf.shardColumnCache().heapBytesCharged());
                assertFalse("nothing was allocated for the column", leaf.isColumnFullyLoaded("rating"));
                assertNodeGaugeDelta(0L, 1L);

                // The reader stays usable; a second column of the same
                // size is refused the same way and counted again.
                NumericDocValues id = leavesOf(reader).get(2).getNumericDocValues("id");
                expectThrows(CircuitBreakingException.class, () -> id.advanceExact(0));
                assertEquals(0L, breaker.getUsed());
                assertNodeGaugeDelta(0L, 2L);
            }
            assertEquals(0L, breaker.getUsed());
        }
    }

    public void testKeywordLoadChargesOrdinalsThenTermsAndReleasesBoth() throws Exception {
        LimitedBreaker breaker = new LimitedBreaker(Long.MAX_VALUE);
        try (Lease lease = acquire()) {
            try (LanceDirectoryReader reader = openHeap(lease.snapshot(), allFragments, breaker)) {
                LanceFragmentLeafReader leaf = leavesOf(reader).get(1);
                SortedDocValues category = leaf.getSortedDocValues("category");
                assertTrue(category.advanceExact(0));
                assertEquals("c1", category.lookupOrd(category.ordValue()).utf8ToString());
                assertEquals(3, category.getValueCount());
                long expected = categoryBytesOfAllFragments();
                assertEquals("ordinal arrays plus dictionaries of every fragment", expected, breaker.getUsed());
                assertEquals(2, breaker.charges.size());
                assertEquals(FRAGMENTS * LanceShardColumnCache.intArrayBytes(ROWS), breaker.charges.get(0).longValue());
                assertEquals(FRAGMENTS * LanceShardColumnCache.termsHeapBytes(3, 6L), breaker.charges.get(1).longValue());
                assertNodeGaugeDelta(expected, 0L);
            }
            assertEquals(0L, breaker.getUsed());
            assertNodeGaugeDelta(0L, 0L);
        }
    }

    public void testKeywordLoadRefusedAtTheTermsStepGivesTheOrdinalsBack() throws Exception {
        long ordinals = FRAGMENTS * LanceShardColumnCache.intArrayBytes(ROWS);
        // The ordinal arrays fit, the dictionaries on top do not.
        LimitedBreaker breaker = new LimitedBreaker(ordinals + 8L);
        try (Lease lease = acquire()) {
            try (LanceDirectoryReader reader = openHeap(lease.snapshot(), allFragments, breaker)) {
                LanceFragmentLeafReader leaf = leavesOf(reader).get(0);
                SortedDocValues category = leaf.getSortedDocValues("category");
                CircuitBreakingException refused = expectThrows(CircuitBreakingException.class, () -> category.advanceExact(0));
                assertTrue(refused.getMessage(), refused.getMessage().contains("column [category]"));
                assertEquals("the first step's charge came back with the refusal", 0L, breaker.getUsed());
                assertEquals(0L, leaf.shardColumnCache().heapBytesCharged());
                assertEquals(1L, leaf.shardColumnCache().heapScanCount());
                assertNodeGaugeDelta(0L, 1L);
            }
            assertEquals(0L, breaker.getUsed());
        }
    }

    public void testKeywordArrayLoadChargesOuterArraysThenRowsAndTerms() throws Exception {
        LimitedBreaker breaker = new LimitedBreaker(Long.MAX_VALUE);
        try (Lease lease = acquire()) {
            try (LanceDirectoryReader reader = openHeap(lease.snapshot(), allFragments, breaker)) {
                LanceFragmentLeafReader leaf = leavesOf(reader).get(0);
                SortedSetDocValues tags = leaf.getSortedSetDocValues("tags");
                assertTrue(tags.advanceExact(0));
                assertEquals("row 0 carries t0 twice", 1, tags.docValueCount());
                assertEquals(5, tags.getValueCount());
                long expected = tagsBytesOfAllFragments();
                assertEquals(expected, breaker.getUsed());
                assertEquals(2, breaker.charges.size());
                assertEquals(FRAGMENTS * LanceShardColumnCache.objectArrayBytes(ROWS), breaker.charges.get(0).longValue());
                assertNodeGaugeDelta(expected, 0L);
            }
            assertEquals(0L, breaker.getUsed());
        }
    }

    public void testBooleanHeapLoadIsChargedLikeNumeric() throws Exception {
        LimitedBreaker breaker = new LimitedBreaker(numericBytesOfAllFragments() - 1L);
        try (Lease lease = acquire()) {
            try (LanceDirectoryReader reader = openHeap(lease.snapshot(), allFragments, breaker)) {
                NumericDocValues flag = leavesOf(reader).get(0).getNumericDocValues("flag");
                CircuitBreakingException refused = expectThrows(CircuitBreakingException.class, () -> flag.advanceExact(0));
                assertTrue(refused.getMessage(), refused.getMessage().contains("column [flag]"));
                assertEquals(0L, breaker.getUsed());
            }
        }
    }

    public void testSingleFragmentLoadChargesThroughTheCacheAndReleasesOnClose() throws Exception {
        // A sparse hint below the ratio takes the hinted rows; the first
        // doc outside the hint loads this fragment alone into heap
        // (ensureNumericLoaded with the shard cache bypassed), which is
        // charged for one fragment and given back when the reader closes.
        LimitedBreaker breaker = new LimitedBreaker(Long.MAX_VALUE);
        try (Lease lease = acquire()) {
            try (LanceDirectoryReader reader = openHeap(lease.snapshot(), allFragments, breaker)) {
                LanceFragmentLeafReader leaf = leavesOf(reader).get(1);
                leaf.hintMatchedOffsets(new int[] { 3, 17, 44 }, false);
                NumericDocValues rating = leaf.getNumericDocValues("rating");
                assertTrue(rating.advanceExact(3));
                assertTrue(leaf.isServingSparse("rating"));
                assertEquals("the hinted take is not a heap column load", 0L, breaker.getUsed());

                assertTrue(rating.advanceExact(5));
                assertEquals(((ROWS + 5) * 37) % 1000, rating.longValue());
                assertTrue(leaf.isColumnFullyLoaded("rating"));
                assertFalse(leavesOf(reader).get(0).isColumnFullyLoaded("rating"));
                long oneFragment = LanceShardColumnCache.numericHeapBytes(ROWS);
                assertEquals("one fragment charged, not three", oneFragment, breaker.getUsed());
                assertEquals(oneFragment, leaf.shardColumnCache().heapBytesCharged());
                assertNodeGaugeDelta(oneFragment, 0L);
            }
            assertEquals(0L, breaker.getUsed());
            assertNodeGaugeDelta(0L, 0L);
        }
    }

    public void testSingleFragmentLoadRefusedLeavesNothingCharged() throws Exception {
        long oneFragment = LanceShardColumnCache.numericHeapBytes(ROWS);
        LimitedBreaker breaker = new LimitedBreaker(oneFragment - 1L);
        try (Lease lease = acquire()) {
            try (LanceDirectoryReader reader = openHeap(lease.snapshot(), allFragments, breaker)) {
                LanceFragmentLeafReader leaf = leavesOf(reader).get(2);
                leaf.hintMatchedOffsets(new int[] { 3, 17, 44 }, false);
                NumericDocValues rating = leaf.getNumericDocValues("rating");
                assertTrue(rating.advanceExact(3));
                CircuitBreakingException refused = expectThrows(CircuitBreakingException.class, () -> rating.advanceExact(5));
                assertTrue(refused.getMessage(), refused.getMessage().contains("column [rating]"));
                assertEquals(oneFragment, refused.getBytesWanted());
                assertEquals(0L, breaker.getUsed());
                assertFalse(leaf.isColumnFullyLoaded("rating"));
                assertNodeGaugeDelta(0L, 1L);
            }
        }
    }

    public void testSingleFragmentKeywordLoadChargesBothSteps() throws Exception {
        LimitedBreaker breaker = new LimitedBreaker(Long.MAX_VALUE);
        try (Lease lease = acquire()) {
            try (LanceDirectoryReader reader = openHeap(lease.snapshot(), allFragments, breaker)) {
                LanceFragmentLeafReader leaf = leavesOf(reader).get(0);
                // An exclusive sparse hint builds the sparse dictionary; a
                // doc outside it switches this leaf alone to the full one.
                leaf.hintMatchedOffsets(new int[] { 1, 2, 4 }, true);
                SortedDocValues category = leaf.getSortedDocValues("category");
                assertTrue(category.advanceExact(1));
                assertEquals(0L, breaker.getUsed());
                // Row 5 is outside the hint and carries c2, a term of the
                // sparse dictionary, so the instance answers from the full
                // column it loads for this fragment alone.
                assertTrue(category.advanceExact(5));
                assertEquals("c2", category.lookupOrd(category.ordValue()).utf8ToString());
                long expected = LanceShardColumnCache.intArrayBytes(ROWS) + LanceShardColumnCache.termsHeapBytes(3, 6L);
                assertEquals(expected, breaker.getUsed());
                assertEquals(2, breaker.charges.size());
                assertFalse(leavesOf(reader).get(1).isColumnFullyLoaded("category"));
            }
            assertEquals(0L, breaker.getUsed());
        }
    }

    public void testHeapDictionaryTheStoreCouldNotHoldIsChargedOnPublish() throws Exception {
        // A store with a positive budget too small for any dictionary:
        // it scans the keyword column and hands the dictionaries back as
        // heap results, which the cache charges before the leaves see them.
        try (ColumnStore store = new ColumnStore(allocator, 64L)) {
            LimitedBreaker breaker = new LimitedBreaker(Long.MAX_VALUE);
            try (Lease lease = acquire()) {
                try (LanceDirectoryReader reader = openStored(lease.snapshot(), store, breaker)) {
                    LanceFragmentLeafReader leaf = leavesOf(reader).get(0);
                    SortedDocValues category = leaf.getSortedDocValues("category");
                    assertTrue(category.advanceExact(0));
                    assertFalse(leaf.isServingOffHeap("category"));
                    assertEquals(1L, store.budgetMissCount());
                    assertEquals("no scan of its own; the store's dictionaries were charged", 0L, leaf.shardColumnCache().heapScanCount());
                    assertEquals(categoryBytesOfAllFragments(), breaker.getUsed());

                    SortedSetDocValues tags = leavesOf(reader).get(2).getSortedSetDocValues("tags");
                    assertTrue(tags.advanceExact(2));
                    assertEquals(categoryBytesOfAllFragments() + tagsBytesOfAllFragments(), breaker.getUsed());
                }
                assertEquals(0L, breaker.getUsed());
            }
            // A refused publish: the store's scan result is dropped and
            // nothing stays charged.
            LimitedBreaker tight = new LimitedBreaker(8L);
            try (Lease lease = acquire()) {
                try (LanceDirectoryReader reader = openStored(lease.snapshot(), store, tight)) {
                    SortedDocValues category = leavesOf(reader).get(0).getSortedDocValues("category");
                    CircuitBreakingException refused = expectThrows(CircuitBreakingException.class, () -> category.advanceExact(0));
                    assertTrue(refused.getMessage(), refused.getMessage().contains("column [category]"));
                    assertEquals(0L, tight.getUsed());
                }
            }
        }
    }

    public void testReaderWithoutABreakerAllocatesUnchecked() throws Exception {
        // The fragment list open has no production caller and hands the
        // cache a Noop breaker: the load runs, the cache still counts what
        // it would have charged, and the Noop breaker records nothing.
        Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty());
        try (
            LanceDirectoryReader reader = LanceDirectoryReader.openForFragments(
                new ByteBuffersDirectory(),
                null,
                dataset,
                "",
                LancePrimaryKeyType.NONE,
                Collections.emptyMap(),
                new ArrayList<>(allFragments)
            )
        ) {
            LanceFragmentLeafReader leaf = leavesOf(reader).get(0);
            assertTrue(leaf.getNumericDocValues("rating").advanceExact(0));
            assertEquals(numericBytesOfAllFragments(), leaf.shardColumnCache().heapBytesCharged());
            assertEquals(0L, leaf.requestBreaker().getUsed());
            assertEquals(CircuitBreaker.REQUEST, leaf.requestBreaker().getName());
        }
    }

    /**
     * Breaker with a hard limit: a charge that would exceed it throws the
     * exception a {@code ChildMemoryCircuitBreaker} throws, with the same
     * message shape, and records every accepted charge.
     */
    static final class LimitedBreaker implements CircuitBreaker {
        private final AtomicLong used = new AtomicLong();
        private final long limit;
        final List<Long> charges = new ArrayList<>();

        LimitedBreaker(long limit) {
            this.limit = limit;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            throw new CircuitBreakingException(
                "[request] Data too large, data for ["
                    + fieldName
                    + "] would be ["
                    + bytesNeeded
                    + "/"
                    + bytesNeeded
                    + "b], which is larger than the limit of ["
                    + limit
                    + "/"
                    + limit
                    + "b]",
                bytesNeeded,
                limit,
                Durability.TRANSIENT
            );
        }

        @Override
        public double addEstimateBytesAndMaybeBreak(long bytes, String label) {
            long next = used.get() + bytes;
            if (next > limit) {
                circuitBreak(label, bytes);
            }
            used.set(next);
            charges.add(bytes);
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
            return CircuitBreaker.REQUEST;
        }

        @Override
        public Durability getDurability() {
            return Durability.TRANSIENT;
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {}
    }
}
