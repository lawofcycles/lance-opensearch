/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.concurrent.atomic.AtomicLong;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link LanceCircuitBreaker}. Cover the three operations
 * that the plugin's runtime exercises: pushing a fresh
 * {@link org.lance.Session#sizeBytes()} reading into the breaker,
 * flipping the {@code enabled} flag through the settings listener, and
 * refusing to start a scan when the breaker has caught up to its
 * limit.
 *
 * <p>Use a tiny in-test {@link CircuitBreaker} implementation rather
 * than the real {@code ChildMemoryCircuitBreaker} because the goal is
 * to observe the exact bytes the helper hands to
 * {@link CircuitBreaker#addWithoutBreaking(long)} and how
 * {@link CircuitBreaker#getUsed()} vs {@link CircuitBreaker#getLimit()}
 * compare after each call. The real breaker adds thread-safety and
 * parent-breaker plumbing that is not on the code path under test.
 */
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
