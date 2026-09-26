/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.memory.RootAllocator;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.mapper.Uid;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.engine.LanceFragmentSchema.TakeProjection;
import org.opensearch.lance.stats.LanceNodeStats;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * The node's cache of the rows behind hits on its own (the key, the
 * weigher, a partial row, the negative entry, the invalidation of one
 * version) and behind a leaf ({@link LanceStoredFields#prefetchRows}
 * reading and writing it through a snapshot's
 * {@link LanceWarmCache.Snapshot#fetchTable()}).
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceFetchCacheTests extends OpenSearchTestCase {

    private static final List<String> COLUMNS = List.of("id", "body", "rating");

    private static LanceFetchCache cache() {
        return new LanceFetchCache(1024L * 1024, 256L * 1024, true, TimeValue.ZERO);
    }

    private static long address(int fragment, int offset) {
        return ((long) fragment << 32) | offset;
    }

    public void testKeyNamesTheVersionSoAnotherVersionIsAnotherEntry() {
        LanceFetchCache.Key v1 = new LanceFetchCache.Key("uuid", 1L, address(0, 7), "body");
        LanceFetchCache.Key v1Again = new LanceFetchCache.Key("uuid", 1L, address(0, 7), "body");
        LanceFetchCache.Key v2 = new LanceFetchCache.Key("uuid", 2L, address(0, 7), "body");
        assertEquals(v1, v1Again);
        assertEquals(v1.hashCode(), v1Again.hashCode());
        assertNotEquals(v1, v2);
        assertNotEquals(v1, new LanceFetchCache.Key("other", 1L, address(0, 7), "body"));
        assertNotEquals(v1, new LanceFetchCache.Key("uuid", 1L, address(1, 7), "body"));
        assertNotEquals(v1, new LanceFetchCache.Key("uuid", 1L, address(0, 7), "title"));

        LanceFetchCache cache = cache();
        LanceFetchCache.Table first = cache.table("uuid", 1L);
        LanceFetchCache.Table second = cache.table(new LanceWarmCache.SnapshotKey("uuid", 2L));
        first.put(address(0, 7), COLUMNS, new Object[] { 7L, "hello", 3L });
        assertArrayEquals(new Object[] { 7L, "hello", 3L }, first.lookup(address(0, 7), COLUMNS));
        assertNull("the same row at another version is not held", second.lookup(address(0, 7), COLUMNS));
        assertEquals(3, cache.count());
        assertEquals(3L, cache.hitCount());
        assertEquals(3L, cache.missCount());
        assertEquals(1L, cache.rowsServedCount());
    }

    public void testWeigherCountsTheValueAndTheEntryBoundSkipsALargeCell() {
        assertEquals(0L, LanceFetchCache.weightOf(null));
        assertEquals(48L + 2L * 5, LanceFetchCache.weightOf("hello"));
        assertEquals(16L + 3, LanceFetchCache.weightOf(new byte[3]));
        assertEquals(24L, LanceFetchCache.weightOf(42L));
        assertEquals(24L, LanceFetchCache.weightOf(1.5d));
        assertEquals(16L, LanceFetchCache.weightOf(Boolean.TRUE));
        assertEquals(32L, LanceFetchCache.weightOf(new double[] { 1.0d, 2.0d }));
        assertEquals(16L + 4L * 2 + (48L + 2L) + (48L + 4L), LanceFetchCache.weightOf(new String[] { "a", "bb" }));
        Map<String, Object> struct = new LinkedHashMap<>();
        struct.put("region", "eu");
        struct.put("score", 9L);
        assertEquals(48L + (32L + (48L + 12L) + (48L + 4L)) + (32L + (48L + 10L) + 24L), LanceFetchCache.weightOf(struct));
        assertEquals(48L + 24L + 24L, LanceFetchCache.weightOf(List.of(1L, 2L)));

        LanceFetchCache cache = new LanceFetchCache(1024L * 1024, 100L, true, TimeValue.ZERO);
        LanceFetchCache.Table table = cache.table("uuid", 1L);
        String large = "x".repeat(200);
        table.put(address(0, 1), List.of("id", "body"), new Object[] { 1L, large });
        assertEquals("the large cell is not stored, the small one is", 1, cache.count());
        assertNull("a row with an unheld cell is not served", table.lookup(address(0, 1), List.of("id", "body")));
        assertArrayEquals(new Object[] { 1L }, table.lookup(address(0, 1), List.of("id")));
        assertEquals("the key and the entry weigh with the value", 48L + 32L + 24L, cache.weight());
    }

    public void testEvictionAtTheLimitCountsAndKeepsTheWeightUnderIt() {
        // Each Long cell weighs 48 (key) + 32 (entry) + 24 (value) = 104.
        LanceFetchCache cache = new LanceFetchCache(104L * 10, 1024L, true, TimeValue.ZERO);
        LanceFetchCache.Table table = cache.table("uuid", 1L);
        for (int i = 0; i < 25; i++) {
            table.put(address(0, i), List.of("id"), new Object[] { (long) i });
        }
        assertTrue("the weight stays at or under the limit: " + cache.weight(), cache.weight() <= 104L * 10);
        assertEquals(10, cache.count());
        assertEquals(15L, cache.stats().evictions());
    }

    public void testAPartialRowIsNotServedAndItsHeldCellsCountAsHits() {
        LanceFetchCache cache = cache();
        LanceFetchCache.Table table = cache.table("uuid", 1L);
        // A request that projected the key alone left one cell.
        table.put(address(0, 3), List.of("id"), new Object[] { 3L });
        assertNull("body and rating are absent, so the row is taken", table.lookup(address(0, 3), COLUMNS));
        assertEquals(1L, cache.hitCount());
        assertEquals(2L, cache.missCount());
        assertEquals(0L, cache.rowsServedCount());
        // The take of the whole row fills the other cells; the next
        // lookup is served.
        table.put(address(0, 3), COLUMNS, new Object[] { 3L, "text", 5L });
        assertArrayEquals(new Object[] { 3L, "text", 5L }, table.lookup(address(0, 3), COLUMNS));
        assertEquals(4L, cache.hitCount());
        assertEquals(1L, cache.rowsServedCount());
        assertEquals("the key cell was overwritten, not duplicated", 3, cache.count());
        // A null cell (an Arrow null) is held and served as null.
        table.put(address(0, 4), COLUMNS, new Object[] { 4L, null, 6L });
        assertArrayEquals(new Object[] { 4L, null, 6L }, table.lookup(address(0, 4), COLUMNS));
    }

    public void testAMissingRowIsANegativeEntryServedWithoutATake() {
        LanceFetchCache cache = cache();
        LanceFetchCache.Table table = cache.table("uuid", 1L);
        table.putMissing(address(2, 9), COLUMNS);
        assertEquals(3, cache.count());
        assertSame(LanceFetchCache.MISSING_ROW, table.lookup(address(2, 9), COLUMNS));
        assertEquals("every column counts as a hit", 3L, cache.hitCount());
        assertEquals(1L, cache.rowsServedCount());
        assertSame("a projection of one column is missing too", LanceFetchCache.MISSING_ROW, table.lookup(address(2, 9), List.of("body")));
        assertEquals("the negative entries weigh their key and entry alone", 3L * (48L + 32L), cache.weight());
    }

    public void testInvalidatingAVersionDropsItsEntriesOnly() {
        LanceFetchCache cache = cache();
        LanceFetchCache.Table v1 = cache.table("uuid", 1L);
        LanceFetchCache.Table v2 = cache.table("uuid", 2L);
        LanceFetchCache.Table other = cache.table("other", 1L);
        v1.put(address(0, 1), COLUMNS, new Object[] { 1L, "a", 1L });
        v2.put(address(0, 1), COLUMNS, new Object[] { 1L, "a", 1L });
        other.put(address(0, 1), COLUMNS, new Object[] { 1L, "a", 1L });
        assertEquals(9, cache.count());
        assertEquals(3, cache.invalidate("uuid", 1L));
        assertEquals(6, cache.count());
        assertNull(v1.lookup(address(0, 1), COLUMNS));
        assertNotNull(v2.lookup(address(0, 1), COLUMNS));
        assertNotNull(other.lookup(address(0, 1), COLUMNS));
        assertEquals(0, cache.invalidate("uuid", 1L));
        assertEquals(3, cache.invalidateIndexes(List.of("uuid")));
        assertEquals(3, cache.count());
        assertNotNull(other.lookup(address(0, 1), COLUMNS));
        assertEquals(0, cache.invalidateIndexes(List.of()));
        assertEquals(6L, cache.stats().invalidations());
    }

    public void testDisablingDropsEveryEntryAndCountsNothing() {
        LanceFetchCache cache = cache();
        LanceFetchCache.Table table = cache.table("uuid", 1L);
        table.put(address(0, 1), COLUMNS, new Object[] { 1L, "a", 1L });
        assertEquals(3, cache.count());
        cache.setEnabled(false);
        assertFalse(cache.isEnabled());
        assertEquals(0, cache.count());
        assertEquals(0L, cache.weight());
        table.put(address(0, 1), COLUMNS, new Object[] { 1L, "a", 1L });
        table.putMissing(address(0, 2), COLUMNS);
        assertNull(table.lookup(address(0, 1), COLUMNS));
        table.skipped(4);
        assertEquals(0, cache.count());
        LanceNodeStats.FetchCacheStats stats = cache.stats();
        assertFalse(stats.enabled());
        assertEquals("nothing counted while off", 0L, stats.hits());
        assertEquals(0L, stats.misses());
        assertEquals(0L, stats.skipped());
        assertEquals(0L, stats.rowsServed());
        cache.setEnabled(true);
        table.put(address(0, 1), COLUMNS, new Object[] { 1L, "a", 1L });
        assertEquals(3, cache.count());
    }

    public void testExpireDropsAnAgedEntryAsAnEvictionAndAMiss() throws Exception {
        LanceFetchCache cache = new LanceFetchCache(1024L * 1024, 1024L, true, TimeValue.timeValueMillis(1));
        LanceFetchCache.Table table = cache.table("uuid", 1L);
        table.put(address(0, 1), List.of("id"), new Object[] { 1L });
        Thread.sleep(5);
        assertNull(table.lookup(address(0, 1), List.of("id")));
        assertEquals(1L, cache.stats().evictions());
        assertEquals(1L, cache.stats().misses());
        assertEquals(0, cache.count());
        cache.setExpire(TimeValue.ZERO);
        table.put(address(0, 1), List.of("id"), new Object[] { 1L });
        Thread.sleep(5);
        assertNotNull("without an expiry the entry stays", table.lookup(address(0, 1), List.of("id")));
    }

    public void testStatsReportTheSettingTheWeightAndTheCounters() {
        LanceFetchCache cache = new LanceFetchCache(4096L, 1024L, true, TimeValue.ZERO);
        LanceFetchCache.Table table = cache.table("uuid", 1L);
        table.put(address(0, 1), List.of("id"), new Object[] { 1L });
        table.lookup(address(0, 1), List.of("id"));
        table.lookup(address(0, 2), List.of("id"));
        table.skipped(3);
        LanceNodeStats.FetchCacheStats stats = cache.stats();
        assertEquals(new LanceNodeStats.FetchCacheStats(true, 104L, 4096L, 1, 1L, 1L, 0L, 0L, 3L, 1L), stats);
        assertEquals("LanceFetchCache[entries=1, bytes=104, limit=4096]", cache.toString());
    }

    /**
     * A leaf over a snapshot reads and writes the cache: the first
     * prefetch takes and stores, a fresh leaf over the same snapshot
     * renders the same rows without a take, a narrower projection's
     * cells serve a later wider request only for the column they hold,
     * and a leaf under a foreign reader wrapper neither reads nor writes
     * and counts as skipped.
     */
    public void testLeafServesRepeatedRowsFromTheCacheAndSkipsUnderAWrapper() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(scratchDir, "fcache-" + getTestName().toLowerCase(Locale.ROOT), 1, 1_000);
        LanceFetchCache fetchCache = cache();
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            LanceWarmCache warmCache = new LanceWarmCache(allocator, 64L * 1024 * 1024, 8, true, Runnable::run, fetchCache);
            LanceWarmCache.Lease lease = warmCache.acquire(
                "uuid",
                uri,
                StorageOptions.empty(),
                Optional.empty(),
                "id",
                LancePrimaryKeyType.LONG,
                LanceOverrides.EMPTY
            )
        ) {
            LanceWarmCache.Snapshot snapshot = lease.snapshot();
            assertNotNull("the snapshot carries the cache's view of its version", snapshot.fetchTable());
            assertEquals("uuid", snapshot.fetchTable().indexUuid());
            assertEquals(snapshot.version(), snapshot.fetchTable().version());
            LanceFragmentSchema schema = snapshot.schema();
            TakeProjection all = schema.takeProjection();
            List<Integer> fragmentIds = new ArrayList<>();
            for (LanceWarmCache.FragmentMeta meta : snapshot.fragments()) {
                fragmentIds.add(meta.id());
            }

            // First request: the rows are taken and every cell stored.
            LanceNodeStats.FetchStats before = FetchTakeStats.snapshot();
            try (LanceDirectoryReader reader = open(snapshot, fragmentIds)) {
                LanceFragmentLeafReader leaf = leafOf(reader);
                assertSame(snapshot.fetchTable(), ((LanceStoredFields) leaf.storedFields()).fetchCache());
                leaf.setTakeProjection(all);
                leaf.setFetchCacheEligible(true);
                leaf.prefetchRows(new int[] { 1, 8, 8 });
                assertEquals("one take", before.takeCount() + 1, FetchTakeStats.snapshot().takeCount());
                assertEquals(2L * all.columns().size(), fetchCache.count());
                assertEquals("nothing was held before the take", 0L, fetchCache.hitCount());
                assertEquals(2L * all.columns().size(), fetchCache.missCount());
                Collected rendered = new Collected(true, true);
                leaf.materialiseStoredFields(1, rendered);
                assertEquals("1", rendered.id());
                assertEquals("c1", rendered.source().get("category"));
            }

            // Second request over the same snapshot: served from the cache,
            // no take, the same rendering.
            before = FetchTakeStats.snapshot();
            try (LanceDirectoryReader reader = open(snapshot, fragmentIds)) {
                LanceFragmentLeafReader leaf = leafOf(reader);
                leaf.setTakeProjection(all);
                leaf.setFetchCacheEligible(true);
                leaf.prefetchRows(new int[] { 1, 8 });
                assertEquals("no take", before.takeCount(), FetchTakeStats.snapshot().takeCount());
                assertEquals(2L * all.columns().size(), fetchCache.hitCount());
                assertEquals(2L, fetchCache.rowsServedCount());
                Collected rendered = new Collected(true, true);
                leaf.materialiseStoredFields(1, rendered);
                assertEquals("1", rendered.id());
                assertEquals("c1", rendered.source().get("category"));
                assertEquals(37, rendered.source().get("rating"));
            }

            // A narrower projection over other rows stores the key alone;
            // a full request over those rows is a partial hit and takes,
            // after which the third request is served.
            TakeProjection keyOnly = new TakeProjection(List.of("id"), 0, 0);
            before = FetchTakeStats.snapshot();
            try (LanceDirectoryReader reader = open(snapshot, fragmentIds)) {
                LanceFragmentLeafReader leaf = leafOf(reader);
                leaf.setTakeProjection(keyOnly);
                leaf.setFetchCacheEligible(true);
                leaf.prefetchRows(new int[] { 20, 21 });
                assertEquals(before.takeCount() + 1, FetchTakeStats.snapshot().takeCount());
                assertEquals(before.takeColumns() + 1, FetchTakeStats.snapshot().takeColumns());
            }
            long served = fetchCache.rowsServedCount();
            before = FetchTakeStats.snapshot();
            try (LanceDirectoryReader reader = open(snapshot, fragmentIds)) {
                LanceFragmentLeafReader leaf = leafOf(reader);
                leaf.setTakeProjection(all);
                leaf.setFetchCacheEligible(true);
                leaf.prefetchRows(new int[] { 20, 21 });
                assertEquals("a partial hit is taken whole", before.takeCount() + 1, FetchTakeStats.snapshot().takeCount());
                assertEquals(before.takeColumns() + all.columns().size(), FetchTakeStats.snapshot().takeColumns());
                assertEquals(served, fetchCache.rowsServedCount());
            }
            before = FetchTakeStats.snapshot();
            try (LanceDirectoryReader reader = open(snapshot, fragmentIds)) {
                LanceFragmentLeafReader leaf = leafOf(reader);
                leaf.setTakeProjection(all);
                leaf.setFetchCacheEligible(true);
                leaf.prefetchRows(new int[] { 20, 21 });
                assertEquals("the take filled the other cells", before.takeCount(), FetchTakeStats.snapshot().takeCount());
                assertEquals(served + 2, fetchCache.rowsServedCount());
            }

            // A leaf under a foreign wrapper: rows are taken, the cache is
            // neither read nor written, the rows count as skipped.
            long entries = fetchCache.count();
            long hits = fetchCache.hitCount();
            before = FetchTakeStats.snapshot();
            try (LanceDirectoryReader reader = open(snapshot, fragmentIds)) {
                LeafReaderContext ctx = reader.leaves().get(0);
                FilterLeafReader foreign = new FilterLeafReader(ctx.reader()) {
                    @Override
                    public CacheHelper getCoreCacheHelper() {
                        return in.getCoreCacheHelper();
                    }

                    @Override
                    public CacheHelper getReaderCacheHelper() {
                        return in.getReaderCacheHelper();
                    }
                };
                assertFalse(LanceFragmentLeafReader.wrappedOnlyByOwnReaders(foreign));
                assertTrue(LanceFragmentLeafReader.wrappedOnlyByOwnReaders(ctx.reader()));
                LanceFragmentLeafReader leaf = LanceFragmentLeafReader.unwrap(foreign);
                leaf.setTakeProjection(all);
                leaf.setFetchCacheEligible(false);
                leaf.prefetchRows(new int[] { 1, 8, 30 });
                assertEquals("the held rows are taken all the same", before.takeCount() + 1, FetchTakeStats.snapshot().takeCount());
                assertEquals(before.takeRows() + 3, FetchTakeStats.snapshot().takeRows());
                assertEquals("nothing read", hits, fetchCache.hitCount());
                assertEquals("nothing written", entries, fetchCache.count());
                assertEquals(3L, fetchCache.skippedCount());
            }

            // A leaf nobody decided about (the shard engine's) uses the
            // cache neither way and counts nothing.
            before = FetchTakeStats.snapshot();
            try (LanceDirectoryReader reader = open(snapshot, fragmentIds)) {
                LanceFragmentLeafReader leaf = leafOf(reader);
                leaf.setTakeProjection(all);
                leaf.prefetchRows(new int[] { 1, 8 });
                assertEquals(before.takeCount() + 1, FetchTakeStats.snapshot().takeCount());
                assertEquals(hits, fetchCache.hitCount());
                assertEquals(3L, fetchCache.skippedCount());
            }

            // The version's entries go when its snapshot closes.
            assertTrue(fetchCache.count() > 0);
            warmCache.retire("uuid", snapshot.version() + 1);
            assertEquals("a leased snapshot keeps its entries until it closes", entries, fetchCache.count());
            lease.release();
            assertEquals(0, fetchCache.count());
            assertEquals(entries, fetchCache.stats().invalidations());
        }
    }

    private static LanceDirectoryReader open(LanceWarmCache.Snapshot snapshot, List<Integer> fragmentIds) throws Exception {
        return LanceDirectoryReader.openForSnapshot(
            new ByteBuffersDirectory(),
            snapshot,
            null,
            fragmentIds,
            null,
            new NoopCircuitBreaker("test")
        );
    }

    private static LanceFragmentLeafReader leafOf(LanceDirectoryReader reader) {
        LanceFragmentLeafReader leaf = LanceFragmentLeafReader.unwrap(reader.leaves().get(0).reader());
        assertNotNull(leaf);
        return leaf;
    }

    /** A visitor that keeps the {@code _id} and {@code _source} a leaf renders. */
    private static final class Collected extends StoredFieldVisitor {
        private final boolean needsId;
        private final boolean needsSource;
        private String id;
        private byte[] source;

        Collected(boolean needsId, boolean needsSource) {
            this.needsId = needsId;
            this.needsSource = needsSource;
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) {
            if ("_id".equals(fieldInfo.name)) {
                return needsId ? Status.YES : Status.NO;
            }
            if ("_source".equals(fieldInfo.name)) {
                return needsSource ? Status.YES : Status.NO;
            }
            return Status.NO;
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) {
            if ("_id".equals(fieldInfo.name)) {
                id = Uid.decodeId(value);
            } else if ("_source".equals(fieldInfo.name)) {
                source = value;
            }
        }

        String id() {
            return id;
        }

        Map<String, Object> source() {
            if (source == null) {
                return null;
            }
            return XContentHelper.convertToMap(new BytesArray(source), true, XContentType.JSON).v2();
        }
    }
}
