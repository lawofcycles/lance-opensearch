/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.indices.breaker.BreakerSettings;
import org.opensearch.node.Node;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

/**
 * Tests for {@link LanceCircuitBreaker}. Cover the three operations
 * that the plugin's runtime exercises: pushing a fresh
 * {@link org.lance.Session#sizeBytes()} reading into the breaker,
 * flipping the {@code enabled} flag through the settings listener, and
 * refusing to start a scan when the breaker has caught up to its
 * limit; then the two entries the plugin calls at start,
 * {@link LanceCircuitBreaker#breakerSettings} and
 * {@link LanceCircuitBreaker.Sampler#start}.
 *
 * <p>Use a tiny in-test {@link CircuitBreaker} implementation rather
 * than the real {@code ChildMemoryCircuitBreaker} because the goal is
 * to observe the exact bytes the helper hands to
 * {@link CircuitBreaker#addWithoutBreaking(long)} and how
 * {@link CircuitBreaker#getUsed()} vs {@link CircuitBreaker#getLimit()}
 * compare after each call. The real breaker adds thread-safety and
 * parent-breaker plumbing that is not on the code path under test.
 *
 * <p>Thread leak checking is off at the suite level because the sampler
 * test installs a Lance session, whose native runtime threads cannot be
 * shut down from Java.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceCircuitBreakerTests extends OpenSearchTestCase {

    private RecordingBreaker recording;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        LanceCircuitBreaker.resetForTests();
        recording = new RecordingBreaker(1024L * 1024);
        LanceCircuitBreaker.setBreaker(recording);
    }

    @Override
    public void tearDown() throws Exception {
        LanceCircuitBreaker.resetForTests();
        super.tearDown();
    }

    public void testUpdateUsageReportsAbsoluteReadingAsDelta() {
        // First reading after the breaker is installed must be
        // published as an absolute value: the helper's lastReported
        // counter is zeroed on setBreaker.
        LanceCircuitBreaker.updateUsage(500);
        assertEquals(500L, recording.getUsed());
        assertEquals(1, recording.additionsWithoutBreaking);

        // A larger reading turns into a positive delta.
        LanceCircuitBreaker.updateUsage(1_200);
        assertEquals(1_200L, recording.getUsed());
        assertEquals(2, recording.additionsWithoutBreaking);

        // Cache shrinkage (LRU eviction) reports a negative delta so
        // the breaker eventually falls back below its limit.
        LanceCircuitBreaker.updateUsage(700);
        assertEquals(700L, recording.getUsed());
        assertEquals(3, recording.additionsWithoutBreaking);
    }

    public void testUpdateUsageWithoutBreakerInstalledIsNoop() {
        LanceCircuitBreaker.resetForTests();
        // Must not throw when the breaker slot is empty; the plugin
        // may see one polling tick before setCircuitBreaker fires
        // during test-framework restarts.
        LanceCircuitBreaker.updateUsage(1_000);
    }

    public void testCheckAndTripBelowLimitIsSilent() {
        LanceCircuitBreaker.updateUsage(recording.getLimit() - 1);
        // Getting close is fine; the breaker only trips once the
        // reading catches up to the limit exactly or overshoots.
        LanceCircuitBreaker.checkAndTrip("lance_fts_query");
    }

    public void testCheckAndTripAtOrAboveLimitThrows() {
        LanceCircuitBreaker.updateUsage(recording.getLimit());
        CircuitBreakingException e = expectThrows(
            CircuitBreakingException.class,
            () -> LanceCircuitBreaker.checkAndTrip("lance_fts_query")
        );
        assertTrue(e.getMessage(), e.getMessage().contains("lance_fts_query"));
        assertEquals(CircuitBreaker.Durability.TRANSIENT, e.getDurability());

        LanceCircuitBreaker.updateUsage(recording.getLimit() + 10_000);
        expectThrows(CircuitBreakingException.class, () -> LanceCircuitBreaker.checkAndTrip("lance_knn_query"));
    }

    public void testCheckAndTripDisabledSkipsCheck() {
        LanceCircuitBreaker.setEnabled(false);
        LanceCircuitBreaker.updateUsage(recording.getLimit() * 4);
        // Even a wildly over-limit reading must not trip when the
        // operator has disabled the breaker for an investigation.
        LanceCircuitBreaker.checkAndTrip("lance_fts_query");
    }

    public void testCheckAndTripWithoutBreakerIsNoop() {
        LanceCircuitBreaker.resetForTests();
        LanceCircuitBreaker.checkAndTrip("lance_fts_query");
    }

    public void testCheckAndTripWithNonPositiveLimitIsNoop() {
        LanceCircuitBreaker.resetForTests();
        LanceCircuitBreaker.setBreaker(new RecordingBreaker(0L));
        LanceCircuitBreaker.updateUsage(1_000_000L);
        // A zero-limit breaker matches an operator who deliberately
        // configured no cap; treat it as "no breaker" rather than
        // trip-on-every-call.
        LanceCircuitBreaker.checkAndTrip("lance_fts_query");
    }

    public void testInstallingANewBreakerResetsLastReported() {
        LanceCircuitBreaker.updateUsage(700);
        RecordingBreaker fresh = new RecordingBreaker(1024L * 1024);
        LanceCircuitBreaker.setBreaker(fresh);
        LanceCircuitBreaker.updateUsage(400);
        // Second call after re-install must report 400 as an absolute
        // reading, not (400 - 700) = -300.
        assertEquals(400L, fresh.getUsed());
    }

    public void testBreakerSettingsMirrorTheNativeMemoryLimit() {
        Settings settings = Settings.builder().put("plugins.lance.native_memory.limit", "3gb").build();
        BreakerSettings breaker = LanceCircuitBreaker.breakerSettings(settings);
        assertEquals("lance_native", breaker.getName());
        assertEquals(3L << 30, breaker.getLimit());
        assertEquals(1.0, breaker.getOverhead(), 0.0);
        assertEquals(CircuitBreaker.Type.MEMORY, breaker.getType());
        assertEquals(CircuitBreaker.Durability.TRANSIENT, breaker.getDurability());
    }

    public void testSamplerStartAppliesTheEnabledFlagAndFollowsItsUpdates() {
        Settings settings = Settings.builder().put("plugins.lance.native_memory.circuit_breaker.enabled", false).build();
        ClusterSettings clusterSettings = LanceTestSettings.clusterSettings(settings);
        DeterministicTaskQueue queue = taskQueue();
        try (
            LanceCircuitBreaker.Sampler ignored = LanceCircuitBreaker.Sampler.start(
                queue.getThreadPool(),
                settings,
                clusterSettings,
                () -> 0L
            )
        ) {
            assertFalse("start applies the node setting", LanceCircuitBreaker.isEnabled());
            clusterSettings.applySettings(Settings.builder().put("plugins.lance.native_memory.circuit_breaker.enabled", true).build());
            assertTrue("the dynamic update reaches setEnabled", LanceCircuitBreaker.isEnabled());
        }
    }

    public void testSamplerTickReportsTheSessionAndColumnBytesAndStopsAtClose() {
        LanceRegistry.initSession(
            NativeMemoryLimit.IndexCacheSizing.ofCapacity(64L * 1024 * 1024, NativeMemoryLimit.availableCpus()),
            8L * 1024 * 1024
        );
        try {
            AtomicLong columnBytes = new AtomicLong(12_345L);
            Settings settings = Settings.EMPTY;
            ClusterSettings clusterSettings = LanceTestSettings.clusterSettings(settings);
            DeterministicTaskQueue queue = taskQueue();
            LanceCircuitBreaker.Sampler sampler = LanceCircuitBreaker.Sampler.start(
                queue.getThreadPool(),
                settings,
                clusterSettings,
                columnBytes::get
            );
            assertEquals("nothing is reported before the first tick", 0L, recording.getUsed());
            assertTrue("the first sample is scheduled at the poll interval", queue.hasDeferredTasks());

            tick(queue);
            long sessionBytes = LanceRegistry.currentSession().sizeBytes();
            assertEquals("one tick reports Session.sizeBytes() plus the column bytes", sessionBytes + 12_345L, recording.getUsed());
            assertEquals(1, recording.additionsWithoutBreaking);

            columnBytes.set(20_000L);
            tick(queue);
            assertEquals("the next tick follows the column cache", sessionBytes + 20_000L, recording.getUsed());

            sampler.close();
            columnBytes.set(99_999L);
            tick(queue);
            assertEquals("no tick reports after close", sessionBytes + 20_000L, recording.getUsed());
            assertEquals(2, recording.additionsWithoutBreaking);
        } finally {
            LanceRegistry.closeSession();
        }
    }

    public void testSamplerReschedulesOnlyWhenTheIntervalChanges() throws Exception {
        List<TimeValue> scheduled = new ArrayList<>();
        List<Scheduler.Cancellable> tasks = new ArrayList<>();
        ThreadPool threadPool = new TestThreadPool(getTestName()) {
            @Override
            public Scheduler.Cancellable scheduleWithFixedDelay(Runnable command, TimeValue interval, String executor) {
                Scheduler.Cancellable task = super.scheduleWithFixedDelay(command, interval, executor);
                scheduled.add(interval);
                tasks.add(task);
                return task;
            }
        };
        try {
            // An hour between samples: no tick fires during the test, so
            // every observation is about the schedule itself.
            Settings settings = Settings.builder().put("plugins.lance.native_memory.circuit_breaker.poll_interval", "1h").build();
            ClusterSettings clusterSettings = LanceTestSettings.clusterSettings(settings);
            LanceCircuitBreaker.Sampler sampler = LanceCircuitBreaker.Sampler.start(threadPool, settings, clusterSettings, () -> 0L);
            assertEquals(List.of(TimeValue.timeValueHours(1)), scheduled);

            sampler.setInterval(TimeValue.timeValueHours(1));
            assertEquals("the same interval keeps the schedule", 1, tasks.size());
            assertFalse(tasks.get(0).isCancelled());

            clusterSettings.applySettings(
                Settings.builder().put("plugins.lance.native_memory.circuit_breaker.poll_interval", "30m").build()
            );
            assertEquals(
                "a new interval replaces the schedule",
                List.of(TimeValue.timeValueHours(1), TimeValue.timeValueMinutes(30)),
                scheduled
            );
            assertTrue("the old schedule is cancelled", tasks.get(0).isCancelled());
            assertFalse(tasks.get(1).isCancelled());

            sampler.close();
            assertTrue("close cancels the current schedule", tasks.get(1).isCancelled());
            assertEquals(2, tasks.size());
        } finally {
            ThreadPool.terminate(threadPool, 30L, TimeUnit.SECONDS);
        }
    }

    private DeterministicTaskQueue taskQueue() {
        return new DeterministicTaskQueue(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "sampler").build(), random());
    }

    /** Move the queue's clock to the next scheduled sample and run it. */
    private static void tick(DeterministicTaskQueue queue) {
        queue.advanceTime();
        queue.runAllRunnableTasks();
    }

    /**
     * Minimal {@link CircuitBreaker} that tracks bytes added and the
     * number of {@code addWithoutBreaking} calls. Enough surface for
     * the helper to talk to but no more.
     */
    private static final class RecordingBreaker implements CircuitBreaker {
        private final AtomicLong used = new AtomicLong(0);
        private final long limit;
        int additionsWithoutBreaking = 0;

        RecordingBreaker(long limit) {
            this.limit = limit;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            // not exercised
        }

        @Override
        public double addEstimateBytesAndMaybeBreak(long bytes, String label) {
            used.addAndGet(bytes);
            return 1.0;
        }

        @Override
        public long addWithoutBreaking(long bytes) {
            additionsWithoutBreaking++;
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
        public void setLimitAndOverhead(long limit, double overhead) {
            // not exercised
        }
    }
}
