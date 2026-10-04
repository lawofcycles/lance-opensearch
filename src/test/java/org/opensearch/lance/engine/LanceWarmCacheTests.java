/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.arrow.memory.RootAllocator;
import org.lance.Dataset;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.LanceTestSettings;
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
        return cache.acquire(indexUuid, uri, StorageOptions.empty(), version, "", LancePrimaryKeyType.NONE, LanceOverrides.EMPTY);
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

    public void testFromSettingsReadsTheNodeSettingsAndFollowsTheirUpdates() throws Exception {
        cache.close();
        Settings settings = Settings.builder().put("plugins.lance.cache.enabled", true).put("plugins.lance.cache.max_snapshots", 1).build();
        ClusterSettings clusterSettings = LanceTestSettings.clusterSettings(settings);
        // The statistics collection runs on the executor handed in; inline
        // here, so the build of a snapshot pays the collection and its delay.
        cache = LanceWarmCache.fromSettings(settings, clusterSettings, allocator, 16L * 1024 * 1024, Runnable::run, null);

        assertTrue(cache.isEnabled());
        assertEquals("the column store takes the budget handed in", 16L * 1024 * 1024, cache.columnStore().limitBytes());
        long version = latestVersion();
        Snapshot a;
        try (Lease lease = acquire(UUID_A, Optional.of(version))) {
            a = lease.snapshot();
        }
        try (Lease lease = acquire(UUID_B, Optional.of(version))) {
            assertFalse(lease.snapshot().isClosed());
        }
        assertEquals("max_snapshots from the settings keeps one snapshot", 1, cache.snapshotCount());
        assertTrue("the earlier snapshot was evicted", a.isClosed());

        clusterSettings.applySettings(Settings.builder().put("plugins.lance.test.statistics_collect_delay", "300ms").build());
        String other = LanceTableFactory.writeHintFixtureTable(createTempDir(), "warm-other-" + getTestName(), 1, 50);
        long start = System.nanoTime();
        try (
            Lease lease = cache.acquire(
                "index-c-uuid",
                other,
                StorageOptions.empty(),
                Optional.empty(),
                "",
                LancePrimaryKeyType.NONE,
                LanceOverrides.EMPTY
            )
        ) {
            assertNotNull(cache.tableStatistics().peek(lease.snapshot().dataset().uri(), lease.snapshot().dataset().version()));
        }
        assertTrue(
            "the delay consumer is registered: the inline collection of the new table waited for it",
            System.nanoTime() - start >= TimeValue.timeValueMillis(250).nanos()
        );

        clusterSettings.applySettings(
            Settings.builder().put("plugins.lance.test.statistics_collect_delay", "300ms").put("plugins.lance.cache.enabled", false).build()
        );
        assertFalse("the enabled consumer is registered", cache.isEnabled());
        assertEquals("turning the cache off retires every snapshot", 0, cache.snapshotCount());
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
                .acquire(replacement.key(), replacement.dataset(), "rating", false, fragmentRows, FragmentGroupScan.SEQUENTIAL, null);
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

    public void testAFailedLatestVersionProbeFallsBackToOpeningTheTable() throws Exception {
        // A shard reopened after its table went missing acquires with no
        // version while an idle snapshot of the index is still cached.
        // The probe on that snapshot's dataset fails; the open that follows
        // reports the table as not found, the same error a first open
        // reports, and the idle snapshot stays for when the table is back.
        Snapshot idle;
        try (Lease lease = acquire(UUID_A, Optional.empty())) {
            idle = lease.snapshot();
        }
        Path table = Path.of(uri);
        Path moved = table.resolveSibling(table.getFileName() + ".moved");
        Files.move(table, moved);
        try {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> acquire(UUID_A, Optional.empty()));
            assertTrue(e.getMessage(), e.getMessage().contains("was not found"));
            assertEquals("the probe released its reference", 0, idle.refCount());
            assertFalse(idle.isClosed());
        } finally {
            Files.move(moved, table);
        }
        try (Lease lease = acquire(UUID_A, Optional.empty())) {
            assertSame("the table is back and the idle snapshot serves it again", idle, lease.snapshot());
        }
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

    public void testCloseWaitsForTheLeasesBeforeClosingTheDatasets() throws Exception {
        // A request in the middle of a scan holds a lease. The close of
        // the cache (the plugin closing with the node) must not pull the
        // dataset from under it: it waits for the release, then closes.
        Snapshot idle;
        try (Lease lease = acquire(UUID_B, Optional.empty())) {
            idle = lease.snapshot();
        }
        Lease held = acquire(UUID_A, Optional.empty());
        Snapshot snapshot = held.snapshot();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread closer = new Thread(() -> {
            try {
                cache.close(30_000L);
            } catch (Throwable t) {
                failure.set(t);
            }
        }, getTestName() + "-closer");
        closer.start();
        assertBusy(() -> assertEquals("close has begun and retired what it will close", 0, cache.snapshotCount()));
        assertTrue("close is waiting for the lease", closer.isAlive());
        assertTrue("close retires what it will close", snapshot.isRetired());
        assertTrue("a snapshot nobody holds waits with the rest", idle.isRetired());
        assertFalse("the held snapshot's dataset stays open while the lease is out", snapshot.isClosed());
        assertFalse(snapshot.dataset().getFragments().isEmpty());
        IllegalStateException refused = expectThrows(IllegalStateException.class, () -> acquire(UUID_A, Optional.of(snapshot.version())));
        assertTrue(refused.getMessage(), refused.getMessage().contains("closed"));

        held.release();
        closer.join(TimeUnit.SECONDS.toMillis(30));
        assertFalse("close returned once the lease was released", closer.isAlive());
        assertNull(failure.get());
        assertTrue(snapshot.isClosed());
        assertTrue(idle.isClosed());
        assertEquals(2L, cache.snapshotCloseCount());
        cache = null;
    }

    public void testCloseGivesUpOnALeaseAfterTheWait() throws Exception {
        Lease held = acquire(UUID_A, Optional.empty());
        Snapshot snapshot = held.snapshot();
        long started = System.nanoTime();
        cache.close(200L);
        long waitedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        assertTrue("close waited for the lease: " + waitedMillis + " ms", waitedMillis >= 200L);
        assertTrue("the wait ran out and the dataset was closed under the request", snapshot.isClosed());
        assertEquals("the lease is still counted", 1, snapshot.refCount());
        held.release();
        assertEquals(0, snapshot.refCount());
        assertEquals("the release after the forced close does not close the snapshot a second time", 1L, cache.snapshotCloseCount());
        cache = null;
    }

    public void testCloseDuringABuildRefusesTheSnapshotAndClosesItsDataset() throws Exception {
        // An acquire that passed the guard at its top is building a
        // snapshot when close() runs. close() empties the map and closes
        // only what it took out, so the build must not be filed after it:
        // nothing would close that dataset. The acquire is refused and
        // closes what it built.
        CountDownLatch built = new CountDownLatch(1);
        CountDownLatch closeReturned = new CountDownLatch(1);
        cache.beforePublish(() -> {
            built.countDown();
            try {
                assertTrue(closeReturned.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        });
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<IllegalStateException> refused = new AtomicReference<>();
        Thread acquirer = new Thread(() -> {
            try (Lease lease = acquire(UUID_A, Optional.empty())) {
                failure.set(new AssertionError("a lease was handed out after close: " + lease.snapshot().key()));
            } catch (IllegalStateException e) {
                refused.set(e);
            } catch (Throwable t) {
                failure.set(t);
            }
        }, getTestName() + "-acquirer");
        acquirer.start();
        assertTrue("the build reached the publish", built.await(30, TimeUnit.SECONDS));
        assertEquals(1L, cache.snapshotBuildCount());
        cache.close();
        closeReturned.countDown();
        acquirer.join(TimeUnit.SECONDS.toMillis(30));
        assertFalse(acquirer.isAlive());
        assertNull(failure.get());
        assertNotNull("the acquire was refused", refused.get());
        assertTrue(refused.get().getMessage(), refused.get().getMessage().contains("closed"));
        assertEquals("the built snapshot was not filed", 0, cache.snapshotCount());
        assertEquals("the dataset the build opened was closed", 1L, cache.snapshotCloseCount());
        assertEquals(0, cache.buildLockCount());
        cache = null;
    }

    public void testADeferredFetchDropSparesAReplacementSnapshotOfTheSameKey() throws Exception {
        // The drop of a closed snapshot's fetch cache entries runs on the
        // generic pool. Between its schedule and its run, a request can
        // build a new snapshot for the same (index uuid, version) and
        // fill the cache through it; those cells are the replacement's.
        cache.close();
        List<Runnable> deferred = new CopyOnWriteArrayList<>();
        LanceFetchCache fetchCache = new LanceFetchCache(1024L * 1024, 256L * 1024, true, TimeValue.ZERO);
        cache = new LanceWarmCache(allocator, 64L * 1024 * 1024, 64, true, deferred::add, fetchCache);
        long version = latestVersion();
        Snapshot old;
        try (Lease lease = acquire(UUID_A, Optional.of(version))) {
            old = lease.snapshot();
        }
        // The statistics collection the build scheduled is not the drop.
        deferred.clear();
        cache.retire(UUID_A, version + 1);
        assertTrue(old.isClosed());
        assertEquals("the drop of the closed snapshot's entries was deferred", 1, deferred.size());
        Runnable drop = deferred.remove(0);

        Snapshot replacement;
        try (Lease lease = acquire(UUID_A, Optional.of(version))) {
            replacement = lease.snapshot();
            assertNotSame(old, replacement);
            assertEquals(old.key(), replacement.key());
            replacement.fetchTable().put(7L, List.of("id"), new Object[] { 7L });
            drop.run();
            assertArrayEquals(
                "the deferred drop leaves the replacement's cell in place",
                new Object[] { 7L },
                replacement.fetchTable().lookup(7L, List.of("id"))
            );
        }
        assertEquals(1, fetchCache.indexedCount(UUID_A, version));
        assertEquals("nothing was counted as invalidated", 0L, fetchCache.stats().invalidations());

        // With no snapshot left under the key, the drop takes the cells.
        deferred.clear();
        cache.retire(UUID_A, version + 1);
        assertTrue(replacement.isClosed());
        for (Runnable runnable : deferred) {
            runnable.run();
        }
        assertNull(replacement.fetchTable().lookup(7L, List.of("id")));
        assertEquals(0, fetchCache.indexedCount(UUID_A, version));
        assertEquals(1L, fetchCache.stats().invalidations());
    }

    public void testBuildAndLoadMonitorsLeaveWithTheSnapshot() throws Exception {
        // Both maps of monitors would otherwise keep one entry per
        // (index uuid, version) (and per column) a node has ever read.
        assertEquals(0, cache.buildLockCount());
        assertEquals(0, cache.columnStore().loadLockCount());
        Snapshot snapshot;
        try (Lease lease = acquire(UUID_A, Optional.empty())) {
            snapshot = lease.snapshot();
            assertEquals("the build monitor leaves with the build", 0, cache.buildLockCount());
            Map<Integer, Integer> fragmentRows = new HashMap<>();
            for (LanceWarmCache.FragmentMeta meta : snapshot.fragments()) {
                fragmentRows.put(meta.id(), meta.physicalRows());
            }
            Map<Integer, CachedColumn> rating = cache.columnStore()
                .acquire(snapshot.key(), snapshot.dataset(), "rating", false, fragmentRows, FragmentGroupScan.SEQUENTIAL, null);
            cache.columnStore().unpin(rating.values());
            assertEquals("a load monitor stays for the snapshot's next load of the column", 1, cache.columnStore().loadLockCount());
        }
        cache.retire(UUID_A, snapshot.version() + 1);
        assertTrue(snapshot.isClosed());
        assertEquals(0, cache.buildLockCount());
        assertEquals("the load monitors of a closed snapshot go with its columns", 0, cache.columnStore().loadLockCount());
    }
}
