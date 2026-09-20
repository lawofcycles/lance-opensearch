/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.memory.RootAllocator;
import org.lance.Dataset;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.engine.LanceWarmCache.Lease;
import org.opensearch.lance.engine.LanceWarmCache.Snapshot;
import org.opensearch.lance.engine.LanceWarmCache.SnapshotKey;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Snapshot lifecycle of {@link LanceWarmCache}: one dataset open and one
 * schema pass per (index uuid, version), latest-version probing for
 * unpinned indexes, retire on table advance and index delete, the
 * snapshot bound, and the transient path when the cache is disabled.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceWarmCacheTests extends OpenSearchTestCase {

    private static final String UUID_A = "index-a-uuid";
    private static final String UUID_B = "index-b-uuid";

    private RootAllocator allocator;
    private LanceWarmCache cache;
    private String uri;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Path scratchDir = createTempDir();
        uri = LanceTableFactory.writeHintFixtureTable(scratchDir, "warm-" + getTestName(), 3, 200);
        allocator = new RootAllocator(Long.MAX_VALUE);
        cache = new LanceWarmCache(allocator, 64L * 1024 * 1024, 64, true);
    }

    @Override
    public void tearDown() throws Exception {
        if (cache != null) {
            cache.close();
        }
        if (allocator != null) {
            allocator.close();
        }
        super.tearDown();
    }

    private Lease acquire(String indexUuid, Optional<Long> version) throws Exception {
        return cache.acquire(indexUuid, uri, StorageOptions.empty(), version, "", LancePrimaryKeyType.NONE, Collections.emptyMap());
    }

    private long latestVersion() {
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            return dataset.version();
        }
    }

    /** Commit a new manifest version by deleting one row, which also gives that fragment a deletion file. */
    private void deleteRow(int id) {
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            dataset.delete("id = " + id);
        }
    }

    public void testAcquireWithTheRequestVersionOpensTheDatasetOnceAndSharesTheSnapshot() throws Exception {
        // The coordinator ships the version it enumerated fragments from
        // with every request, pinned or not; the executor keys on it.
        long version = latestVersion();
        Snapshot first;
        try (Lease lease = acquire(UUID_A, Optional.of(version))) {
            first = lease.snapshot();
            assertEquals(1L, cache.datasetOpenCount());
            assertEquals(1L, cache.snapshotBuildCount());
            assertEquals(1, first.refCount());
            assertEquals(3, first.fragments().size());
            assertEquals(new SnapshotKey(UUID_A, version), first.key());
            assertTrue(first.ftsColumns().contains("body"));
            assertTrue(first.schema().isNumericOrBoolean("rating"));
            assertTrue(first.schema().isBoolean("flag"));
        }
        assertEquals("the lease released its reference", 0, first.refCount());
        assertFalse("an unretired snapshot stays open for the next request", first.isClosed());

        try (Lease again = acquire(UUID_A, Optional.of(version))) {
            assertSame("the same snapshot serves the second request", first, again.snapshot());
            assertEquals("no second dataset open", 1L, cache.datasetOpenCount());
            assertEquals("no second schema pass", 1L, cache.snapshotBuildCount());
            assertEquals(1L, cache.snapshotHitCount());
        }
        assertEquals(1, cache.snapshotCount());
    }

    public void testAcquireWithoutAVersionProbesTheLatestVersionOnTheCachedDataset() throws Exception {
        // Fallback for a request that carries no version: the newest
        // cached dataset answers latestVersion() instead of a new open.
        Snapshot first;
        try (Lease lease = acquire(UUID_A, Optional.empty())) {
            first = lease.snapshot();
            assertEquals(latestVersion(), first.version());
        }
        assertEquals(1L, cache.datasetOpenCount());
        try (Lease again = acquire(UUID_A, Optional.empty())) {
            assertSame(first, again.snapshot());
        }
        assertEquals("the latest version was read from the open dataset, not from a new open", 1L, cache.datasetOpenCount());
        assertEquals(1L, cache.snapshotBuildCount());
    }

    public void testTableAdvanceProducesANewSnapshotAndRetireClosesTheOldOne() throws Exception {
        Snapshot before;
        try (Lease lease = acquire(UUID_A, Optional.empty())) {
            before = lease.snapshot();
        }
        deleteRow(3);
        long advanced = latestVersion();
        assertTrue(advanced > before.version());

        Snapshot after;
        try (Lease lease = acquire(UUID_A, Optional.empty())) {
            after = lease.snapshot();
            assertNotSame(before, after);
            assertEquals(advanced, after.version());
            // Fragment 0 now carries a deletion file; the bitmap is
            // resolved on first use and masks the deleted row.
            LanceWarmCache.FragmentMeta fragment0 = after.fragment(0);
            assertTrue(fragment0.hasDeletionFile());
            fragment0.resolveLiveDocs(after.dataset());
            assertEquals(199, fragment0.numDocs());
            assertFalse(fragment0.liveDocs().get(3));
            assertTrue(fragment0.liveDocs().get(4));
            assertFalse(after.fragment(1).hasDeletionFile());
        }
        assertEquals("both versions are held until the old one is retired", 2, cache.snapshotCount());
        assertFalse(before.isClosed());

        cache.retire(UUID_A, advanced);
        assertTrue("the version left behind closes at once when no request holds it", before.isClosed());
        assertFalse("the current version stays", after.isClosed());
        assertEquals(1, cache.snapshotCount());
        assertEquals(1L, cache.snapshotCloseCount());

        // A pinned request for the old version after retire builds a
        // fresh snapshot rather than reviving the closed one.
        try (Lease pinned = acquire(UUID_A, Optional.of(before.version()))) {
            assertNotSame(before, pinned.snapshot());
            assertFalse(pinned.snapshot().isClosed());
        }
    }

    public void testRetireWaitsForTheLastLease() throws Exception {
        Lease held = acquire(UUID_A, Optional.empty());
        Snapshot snapshot = held.snapshot();
        cache.retire(UUID_A, snapshot.version() + 100);
        assertTrue(snapshot.isRetired());
        assertFalse("a referenced snapshot is not closed under a running request", snapshot.isClosed());
        assertFalse(snapshot.dataset().getFragments().isEmpty());

        // A new request for the same key does not reuse a retired snapshot.
        try (Lease fresh = acquire(UUID_A, Optional.of(snapshot.version()))) {
            assertNotSame(snapshot, fresh.snapshot());
        }

        held.release();
        assertTrue("the last release closes a retired snapshot", snapshot.isClosed());
        held.release();
        assertEquals("release is idempotent", 0, snapshot.refCount());
    }

    public void testRetireAllClosesEverySnapshotOfTheIndexOnly() throws Exception {
        long version = latestVersion();
        Snapshot a;
        Snapshot b;
        try (Lease leaseA = acquire(UUID_A, Optional.of(version)); Lease leaseB = acquire(UUID_B, Optional.of(version))) {
            a = leaseA.snapshot();
            b = leaseB.snapshot();
        }
        assertEquals(2, cache.snapshotCount());
        cache.retireAll(UUID_A);
        assertTrue(a.isClosed());
        assertFalse(b.isClosed());
        assertEquals(1, cache.snapshotCount());
    }

    public void testSnapshotBoundEvictsTheLeastRecentlyUsedIdleSnapshot() throws Exception {
        cache.close();
        cache = new LanceWarmCache(allocator, 64L * 1024 * 1024, 2, true);
        long version = latestVersion();
        Snapshot a;
        Snapshot b;
        try (Lease lease = acquire(UUID_A, Optional.of(version))) {
            a = lease.snapshot();
        }
        try (Lease lease = acquire(UUID_B, Optional.of(version))) {
            b = lease.snapshot();
        }
        // Touch A so B is the least recently used.
        try (Lease lease = acquire(UUID_A, Optional.of(version))) {
            assertSame(a, lease.snapshot());
        }
        Snapshot c;
        try (Lease lease = acquire("index-c-uuid", Optional.of(version))) {
            c = lease.snapshot();
        }
        assertEquals(2, cache.snapshotCount());
        assertTrue("B was least recently used", b.isClosed());
        assertFalse(a.isClosed());
        assertFalse(c.isClosed());
    }

    public void testSnapshotBoundNeverEvictsAReferencedSnapshot() throws Exception {
        cache.close();
        cache = new LanceWarmCache(allocator, 64L * 1024 * 1024, 1, true);
        long version = latestVersion();
        try (Lease held = acquire(UUID_A, Optional.of(version))) {
            try (Lease other = acquire(UUID_B, Optional.of(version))) {
                assertFalse(held.snapshot().isClosed());
                assertFalse(other.snapshot().isClosed());
                assertEquals("both are referenced, so the bound is exceeded rather than a live snapshot closed", 2, cache.snapshotCount());
            }
        }
    }

    public void testDisabledCacheServesATransientSnapshotPerRequest() throws Exception {
        cache.setEnabled(false);
        Snapshot transientSnapshot;
        try (Lease lease = acquire(UUID_A, Optional.empty())) {
            transientSnapshot = lease.snapshot();
            assertFalse(transientSnapshot.isCached());
            assertEquals(0, cache.snapshotCount());
        }
        assertTrue("a transient snapshot closes with its lease", transientSnapshot.isClosed());
        try (Lease lease = acquire(UUID_A, Optional.empty())) {
            assertNotSame(transientSnapshot, lease.snapshot());
        }
        assertEquals("every request opened the table itself", 2L, cache.datasetOpenCount());
        assertEquals(2L, cache.snapshotBuildCount());
    }

    public void testDisablingRetiresEverySnapshot() throws Exception {
        Snapshot idle;
        try (Lease lease = acquire(UUID_A, Optional.empty())) {
            idle = lease.snapshot();
        }
        Lease held = acquire(UUID_B, Optional.empty());
        cache.setEnabled(false);
        assertTrue(idle.isClosed());
        assertTrue(held.snapshot().isRetired());
        assertFalse(held.snapshot().isClosed());
        held.release();
        assertTrue(held.snapshot().isClosed());
        assertEquals(0, cache.snapshotCount());

        cache.setEnabled(true);
        try (Lease lease = acquire(UUID_A, Optional.empty())) {
            assertTrue(lease.snapshot().isCached());
        }
        assertEquals(1, cache.snapshotCount());
    }

    public void testARetiredSnapshotStillLeasedKeepsTheColumnsOfItsReplacement() throws Exception {
        // The shard engine's reader leases its snapshot for as long as it
        // is open. Turning the cache off and on again retires that
        // snapshot without releasing it, and the next fragment path
        // request builds a replacement under the same key. When the old
        // lease is finally released, the replacement's store columns stay:
        // the key names the same rows for both.
        Lease engine = acquire(UUID_A, Optional.empty());
        Snapshot old = engine.snapshot();
        cache.setEnabled(false);
        cache.setEnabled(true);
        assertTrue(old.isRetired());
        assertEquals(1, cache.retiredSnapshotCount());

        Snapshot replacement;
        try (Lease request = acquire(UUID_A, Optional.empty())) {
            replacement = request.snapshot();
            assertNotSame(old, replacement);
            assertEquals(old.key(), replacement.key());
            assertEquals("the old snapshot is no longer counted as held", 1, cache.snapshotCount());
            Map<Integer, Integer> fragmentRows = new HashMap<>();
            for (LanceWarmCache.FragmentMeta meta : replacement.fragments()) {
                fragmentRows.put(meta.id(), meta.physicalRows());
            }
            Map<Integer, CachedColumn> rating = cache.columnStore()
                .acquire(replacement.key(), replacement.dataset(), "rating", false, fragmentRows);
            assertNotNull(rating);
            cache.columnStore().unpin(rating.values());
        }
        assertTrue(cache.columnStore().contains(replacement.key(), "rating", 0));

        engine.release();
        assertTrue(old.isClosed());
        assertFalse(replacement.isClosed());
        assertTrue(
            "closing the retired snapshot must not drop the replacement's columns",
            cache.columnStore().contains(replacement.key(), "rating", 0)
        );
        assertEquals(0, cache.retiredSnapshotCount());

        cache.retire(UUID_A, replacement.version() + 1);
        assertTrue(replacement.isClosed());
        assertFalse("the last snapshot of the key drops the columns", cache.columnStore().contains(replacement.key(), "rating", 0));
    }

    public void testCloseReleasesEverySnapshot() throws Exception {
        Snapshot snapshot;
        try (Lease lease = acquire(UUID_A, Optional.empty())) {
            snapshot = lease.snapshot();
        }
        cache.close();
        assertTrue(snapshot.isClosed());
        assertEquals(0, cache.snapshotCount());
        cache = null;
    }
}
