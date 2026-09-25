/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.metadata;

import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.arrow.memory.RootAllocator;
import org.lance.Dataset;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * {@link TableStatisticsCache}: a lookup never collects on its own
 * thread (a miss answers {@code null} and starts one background
 * collection of the key, after which the lookups hit), one collection
 * per (table, version) however many lookups miss meanwhile, a new entry
 * when the version advances with the previous one kept and older ones
 * dropped, the least recently used entry evicted beyond the bound, and,
 * through {@link LanceWarmCache}, the entry of a version collected when
 * the snapshot that reads it is built and released when that snapshot
 * closes.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class TableStatisticsCacheTests extends OpenSearchTestCase {

    private String uri;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Path scratchDir = createTempDir();
        uri = LanceTableFactory.writeHintFixtureTable(scratchDir, "cache-" + getTestName(), 2, 100);
    }

    private Dataset open() {
        return LanceRegistry.openDataset(uri, StorageOptions.empty());
    }

    private Supplier<Dataset> openerAt(long version) {
        return () -> LanceRegistry.openDataset(uri, StorageOptions.empty(), Optional.of(version));
    }

    /** Commit a new manifest version by deleting one row. */
    private void deleteRow(int id) {
        try (Dataset dataset = open()) {
            dataset.delete("id = " + id);
        }
    }

    /** The statistics of the table's current version, collected on this thread through the cache. */
    private static TableStatistics lookupNow(TableStatisticsCache cache, Dataset dataset, Supplier<Dataset> opener) {
        TableStatistics statistics = cache.lookup(dataset.uri(), dataset.version(), opener);
        if (statistics != null) {
            return statistics;
        }
        statistics = cache.lookup(dataset.uri(), dataset.version(), opener);
        assertNotNull("the same thread executor collected on the miss", statistics);
        return statistics;
    }

    /** An executor that holds its tasks until the test runs them, so the collection's timing is the test's. */
    private static final class DeferredExecutor implements Executor {
        final Deque<Runnable> queued = new ArrayDeque<>();

        @Override
        public void execute(Runnable task) {
            queued.addLast(task);
        }

        void runAll() {
            while (!queued.isEmpty()) {
                queued.pollFirst().run();
            }
        }
    }

    public void testMissAnswersNullAtOnceAndOneBackgroundCollectionServesTheNextLookups() {
        DeferredExecutor executor = new DeferredExecutor();
        TableStatisticsCache cache = new TableStatisticsCache(TableStatisticsCache.DEFAULT_MAX_ENTRIES, executor);
        try (Dataset dataset = open()) {
            long version = dataset.version();
            assertNull("a miss does not wait for the collection", cache.lookup(dataset.uri(), version, openerAt(version)));
            assertEquals(1, cache.pendingCount());
            assertEquals(1L, cache.missCount());
            assertEquals(0L, cache.collectCount());
            assertEquals(0, cache.size());
            assertEquals("one collection was queued", 1, executor.queued.size());

            // Further misses of the same key while the collection is
            // pending start nothing more.
            assertNull(cache.lookup(dataset.uri(), version, openerAt(version)));
            assertFalse("a prefetch of a pending key starts nothing", cache.prefetch(dataset.uri(), version, openerAt(version)));
            assertEquals(2L, cache.missCount());
            assertEquals(1, executor.queued.size());
            assertEquals(1, cache.pendingCount());

            executor.runAll();

            assertEquals(0, cache.pendingCount());
            assertEquals(1L, cache.collectCount());
            assertEquals(1, cache.size());
            TableStatistics statistics = cache.lookup(dataset.uri(), version, openerAt(version));
            assertNotNull("the collected entry is served", statistics);
            assertEquals(200L, statistics.rowCount());
            assertSame(statistics, cache.lookup(dataset.uri(), version, openerAt(version)));
            assertSame(statistics, cache.peek(dataset.uri(), version));
            assertEquals(2L, cache.hitCount());
            assertEquals("hits are not misses", 2L, cache.missCount());
            assertFalse("a prefetch of a held key starts nothing", cache.prefetch(dataset.uri(), version, openerAt(version)));
            assertTrue("one collection counts at least one millisecond: " + cache.collectMillisTotal(), cache.collectMillisTotal() >= 1L);
            assertEquals("the collection opened and closed its own dataset", 0, executor.queued.size());
        }
    }

    public void testPrefetchCollectsWithoutALookup() {
        DeferredExecutor executor = new DeferredExecutor();
        TableStatisticsCache cache = new TableStatisticsCache(TableStatisticsCache.DEFAULT_MAX_ENTRIES, executor);
        long version;
        String tableUri;
        try (Dataset dataset = open()) {
            version = dataset.version();
            tableUri = dataset.uri();
        }
        assertTrue(cache.prefetch(tableUri, version, openerAt(version)));
        assertEquals(1, cache.pendingCount());
        assertEquals("a prefetch is not a plan without statistics", 0L, cache.missCount());
        executor.runAll();
        assertEquals(0, cache.pendingCount());
        assertNotNull("the first lookup after the prefetch hits", cache.lookup(tableUri, version, openerAt(version)));
        assertEquals(1L, cache.hitCount());
        assertEquals(0L, cache.missCount());
    }

    public void testAFailedCollectionLeavesTheKeyFreeForTheNextLookup() {
        DeferredExecutor executor = new DeferredExecutor();
        TableStatisticsCache cache = new TableStatisticsCache(TableStatisticsCache.DEFAULT_MAX_ENTRIES, executor);
        long version;
        String tableUri;
        try (Dataset dataset = open()) {
            version = dataset.version();
            tableUri = dataset.uri();
        }
        Supplier<Dataset> failing = () -> { throw new IllegalStateException("table gone"); };
        assertNull(cache.lookup(tableUri, version, failing));
        assertEquals(1, cache.pendingCount());
        executor.runAll();
        assertEquals("the failure released the key", 0, cache.pendingCount());
        assertEquals(0L, cache.collectCount());
        assertEquals(0, cache.size());
        // The next lookup tries again, with an opener that works.
        assertNull(cache.lookup(tableUri, version, openerAt(version)));
        executor.runAll();
        assertNotNull(cache.peek(tableUri, version));
        assertEquals(1L, cache.collectCount());
    }

    public void testBackgroundCollectionOnAnotherThreadWithTheDelayHook() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            TableStatisticsCache cache = new TableStatisticsCache(TableStatisticsCache.DEFAULT_MAX_ENTRIES, pool);
            cache.setCollectDelayMillis(200L);
            long version;
            String tableUri;
            try (Dataset dataset = open()) {
                version = dataset.version();
                tableUri = dataset.uri();
            }
            long startNanos = System.nanoTime();
            assertNull(cache.lookup(tableUri, version, openerAt(version)));
            long lookupMillis = (System.nanoTime() - startNanos) / 1_000_000L;
            assertTrue("the miss did not wait for the delayed collection: " + lookupMillis + " ms", lookupMillis < 200L);
            assertEquals(1, cache.pendingCount());
            assertBusy(() -> {
                assertEquals(0, cache.pendingCount());
                assertNotNull(cache.peek(tableUri, version));
            }, 10, TimeUnit.SECONDS);
            assertEquals(1L, cache.collectCount());
            assertNotNull(cache.lookup(tableUri, version, openerAt(version)));
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    public void testSameVersionCollectsOnce() {
        TableStatisticsCache cache = new TableStatisticsCache();
        try (Dataset dataset = open()) {
            long version = dataset.version();
            assertNull(
                "the miss that starts the collection answers null even when the executor is this thread",
                cache.lookup(dataset.uri(), version, openerAt(version))
            );
            TableStatistics first = cache.lookup(dataset.uri(), version, openerAt(version));
            TableStatistics second = cache.lookup(dataset.uri(), version, openerAt(version));
            assertNotNull(first);
            assertSame(first, second);
            assertEquals(1L, cache.collectCount());
            assertEquals(1L, cache.missCount());
            assertEquals(2L, cache.hitCount());
            assertEquals(1, cache.size());
            assertEquals(0, cache.pendingCount());
            assertEquals(200L, first.rowCount());
            assertSame(first, cache.peek(dataset.uri(), dataset.version()));
            assertNull(cache.peek(dataset.uri(), dataset.version() + 1));
            assertTrue("one collection counts at least one millisecond: " + cache.collectMillisTotal(), cache.collectMillisTotal() >= 1L);
        }
        try (Dataset reopened = open()) {
            assertSame(
                "a second dataset handle at the same version hits",
                cache.peek(reopened.uri(), reopened.version()),
                cache.lookup(reopened.uri(), reopened.version(), openerAt(reopened.version()))
            );
            assertEquals(1L, cache.collectCount());
        }
    }

    public void testWholeMillisRoundsUpAndCountsAtLeastOne() {
        assertEquals(1L, TableStatisticsCache.wholeMillis(0L));
        assertEquals(1L, TableStatisticsCache.wholeMillis(1L));
        assertEquals(1L, TableStatisticsCache.wholeMillis(999_999L));
        assertEquals(1L, TableStatisticsCache.wholeMillis(1_000_000L));
        assertEquals(2L, TableStatisticsCache.wholeMillis(1_000_001L));
        assertEquals(3L, TableStatisticsCache.wholeMillis(2_500_000L));
    }

    public void testVersionAdvanceAddsAnEntryAndKeepsOnePreviousGeneration() {
        TableStatisticsCache cache = new TableStatisticsCache();
        long v1;
        try (Dataset dataset = open()) {
            v1 = dataset.version();
            lookupNow(cache, dataset, openerAt(v1));
        }
        deleteRow(1);
        long v2;
        try (Dataset dataset = open()) {
            v2 = dataset.version();
            assertTrue(v2 > v1);
            TableStatistics second = lookupNow(cache, dataset, openerAt(v2));
            assertEquals(199L, second.rowCount());
            assertEquals(1L, second.deletedRows());
        }
        assertEquals(2L, cache.collectCount());
        assertEquals("both versions held", 2, cache.size());
        assertNotNull(cache.peek(uriOf(), v1));
        assertNotNull(cache.peek(uriOf(), v2));

        deleteRow(2);
        long v3;
        try (Dataset dataset = open()) {
            v3 = dataset.version();
            lookupNow(cache, dataset, openerAt(v3));
        }
        assertEquals(3L, cache.collectCount());
        assertEquals("the oldest generation is dropped", 2, cache.size());
        assertNull(cache.peek(uriOf(), v1));
        assertNotNull(cache.peek(uriOf(), v2));
        assertNotNull(cache.peek(uriOf(), v3));

        cache.release(uriOf(), v2);
        assertEquals(1, cache.size());
        assertNull(cache.peek(uriOf(), v2));
        cache.release(uriOf(), v2);
        assertEquals("releasing a missing entry is a no-op", 1, cache.size());
    }

    public void testLeastRecentlyUsedEntryIsEvictedBeyondTheBound() throws Exception {
        TableStatisticsCache cache = new TableStatisticsCache(2, Runnable::run);
        Path scratchDir = createTempDir();
        String a = LanceTableFactory.writeHintFixtureTable(scratchDir, "a-" + getTestName(), 1, 10);
        String b = LanceTableFactory.writeHintFixtureTable(scratchDir, "b-" + getTestName(), 1, 10);
        String c = LanceTableFactory.writeHintFixtureTable(scratchDir, "c-" + getTestName(), 1, 10);
        String uriA;
        long versionA;
        String uriB;
        long versionB;
        try (Dataset dataset = LanceRegistry.openDataset(a, StorageOptions.empty())) {
            uriA = dataset.uri();
            versionA = dataset.version();
            lookupNow(cache, dataset, () -> LanceRegistry.openDataset(a, StorageOptions.empty(), Optional.of(versionA)));
        }
        try (Dataset dataset = LanceRegistry.openDataset(b, StorageOptions.empty())) {
            uriB = dataset.uri();
            versionB = dataset.version();
            lookupNow(cache, dataset, () -> LanceRegistry.openDataset(b, StorageOptions.empty(), Optional.of(versionB)));
        }
        try (Dataset dataset = LanceRegistry.openDataset(a, StorageOptions.empty())) {
            lookupNow(cache, dataset, () -> LanceRegistry.openDataset(a, StorageOptions.empty(), Optional.of(versionA)));
        }
        try (Dataset dataset = LanceRegistry.openDataset(c, StorageOptions.empty())) {
            long versionC = dataset.version();
            lookupNow(cache, dataset, () -> LanceRegistry.openDataset(c, StorageOptions.empty(), Optional.of(versionC)));
        }
        assertEquals(2, cache.size());
        assertNotNull("a was touched after b", cache.peek(uriA, versionA));
        assertNull("b is the least recently used", cache.peek(uriB, versionB));
    }

    public void testWarmCacheCollectsWhenASnapshotIsBuiltAndReleasesTheEntryWithIt() throws Exception {
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            LanceWarmCache warmCache = new LanceWarmCache(allocator, 64L * 1024 * 1024, 8, true)
        ) {
            TableStatisticsCache cache = warmCache.tableStatistics();
            assertEquals(0, cache.size());
            LanceWarmCache.Lease first = warmCache.acquire(
                "uuid",
                uri,
                StorageOptions.empty(),
                Optional.empty(),
                "",
                LancePrimaryKeyType.NONE,
                LanceOverrides.EMPTY
            );
            long v1 = first.snapshot().version();
            String tableUri = first.snapshot().dataset().uri();
            // Building the snapshot started the collection (on this
            // thread, with the test's executor): the entry is there
            // before any plan asks for it.
            assertEquals("the snapshot build collected the statistics of its version", 1, cache.size());
            assertEquals(1L, cache.collectCount());
            assertEquals(0L, cache.missCount());
            TableStatistics stats1 = cache.lookup(tableUri, v1, openerAt(v1));
            assertNotNull(stats1);
            assertEquals(200L, stats1.rowCount());
            assertEquals(1L, cache.hitCount());

            deleteRow(5);
            LanceWarmCache.Lease second = warmCache.acquire(
                "uuid",
                uri,
                StorageOptions.empty(),
                Optional.empty(),
                "",
                LancePrimaryKeyType.NONE,
                LanceOverrides.EMPTY
            );
            long v2 = second.snapshot().version();
            assertTrue(v2 > v1);
            assertEquals(2, cache.size());
            TableStatistics stats2 = cache.peek(tableUri, v2);
            assertNotNull("the new version's snapshot collected its statistics", stats2);
            assertEquals(199L, stats2.rowCount());

            // A second acquire of a version the cache holds builds no
            // snapshot and collects nothing.
            warmCache.acquire("uuid", uri, StorageOptions.empty(), Optional.of(v2), "", LancePrimaryKeyType.NONE, LanceOverrides.EMPTY)
                .release();
            assertEquals(2L, cache.collectCount());

            // The table moved on: the old snapshot is retired and closes
            // with its last lease, taking its statistics entry with it.
            warmCache.retire("uuid", v2);
            assertEquals("still leased, still cached", 2, cache.size());
            first.release();
            assertEquals(1, cache.size());
            assertNull(cache.peek(tableUri, v1));
            assertSame(stats2, cache.peek(tableUri, v2));
            second.release();
            assertEquals("the live version stays cached with its snapshot", 1, cache.size());
        }
    }

    private String uriOf() {
        try (Dataset dataset = open()) {
            return dataset.uri();
        }
    }
}
