/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.metadata;

import java.nio.file.Path;
import java.util.Optional;

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
 * {@link TableStatisticsCache}: one collection per (table, version), a
 * new entry when the version advances with the previous one kept and
 * older ones dropped, the least recently used entry evicted beyond the
 * bound, and, through {@link LanceWarmCache}, the entry of a version
 * released when the snapshot that read it closes.
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

    /** Commit a new manifest version by deleting one row. */
    private void deleteRow(int id) {
        try (Dataset dataset = open()) {
            dataset.delete("id = " + id);
        }
    }

    public void testSameVersionCollectsOnce() {
        TableStatisticsCache cache = new TableStatisticsCache();
        try (Dataset dataset = open()) {
            TableStatistics first = cache.forDataset(dataset);
            TableStatistics second = cache.forDataset(dataset);
            assertSame(first, second);
            assertEquals(1L, cache.collectCount());
            assertEquals(1L, cache.hitCount());
            assertEquals(1, cache.size());
            assertEquals(200L, first.rowCount());
            assertSame(first, cache.peek(dataset.uri(), dataset.version()));
            assertNull(cache.peek(dataset.uri(), dataset.version() + 1));
            assertTrue(cache.collectMillisTotal() >= 0L);
        }
        try (Dataset reopened = open()) {
            assertSame(
                "a second dataset handle at the same version hits",
                cache.peek(reopened.uri(), reopened.version()),
                cache.forDataset(reopened)
            );
            assertEquals(1L, cache.collectCount());
        }
    }

    public void testVersionAdvanceAddsAnEntryAndKeepsOnePreviousGeneration() {
        TableStatisticsCache cache = new TableStatisticsCache();
        long v1;
        try (Dataset dataset = open()) {
            v1 = dataset.version();
            cache.forDataset(dataset);
        }
        deleteRow(1);
        long v2;
        try (Dataset dataset = open()) {
            v2 = dataset.version();
            assertTrue(v2 > v1);
            TableStatistics second = cache.forDataset(dataset);
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
            cache.forDataset(dataset);
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
        TableStatisticsCache cache = new TableStatisticsCache(2);
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
            cache.forDataset(dataset);
        }
        try (Dataset dataset = LanceRegistry.openDataset(b, StorageOptions.empty())) {
            uriB = dataset.uri();
            versionB = dataset.version();
            cache.forDataset(dataset);
        }
        try (Dataset dataset = LanceRegistry.openDataset(a, StorageOptions.empty())) {
            cache.forDataset(dataset);
        }
        try (Dataset dataset = LanceRegistry.openDataset(c, StorageOptions.empty())) {
            cache.forDataset(dataset);
        }
        assertEquals(2, cache.size());
        assertNotNull("a was touched after b", cache.peek(uriA, versionA));
        assertNull("b is the least recently used", cache.peek(uriB, versionB));
    }

    public void testWarmCacheReleasesTheEntryWithItsSnapshot() throws Exception {
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            LanceWarmCache warmCache = new LanceWarmCache(allocator, 64L * 1024 * 1024, 8, true)
        ) {
            TableStatisticsCache cache = warmCache.tableStatistics();
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
            TableStatistics stats1 = cache.forVersion(first.snapshot().dataset().uri(), v1, first.snapshot().dataset());
            assertEquals(200L, stats1.rowCount());
            assertEquals(1, cache.size());

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
            TableStatistics stats2 = cache.forVersion(second.snapshot().dataset().uri(), v2, second.snapshot().dataset());
            assertEquals(199L, stats2.rowCount());
            assertEquals(2, cache.size());

            // The table moved on: the old snapshot is retired and closes
            // with its last lease, taking its statistics entry with it.
            warmCache.retire("uuid", v2);
            assertEquals("still leased, still cached", 2, cache.size());
            first.release();
            assertEquals(1, cache.size());
            assertNull(cache.peek(second.snapshot().dataset().uri(), v1));
            assertSame(stats2, cache.peek(second.snapshot().dataset().uri(), v2));
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
