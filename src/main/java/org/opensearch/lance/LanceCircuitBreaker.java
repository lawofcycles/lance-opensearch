/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;

/**
 * Node-scoped bridge between the Lance {@link org.lance.Session} native
 * caches and OpenSearch's {@link CircuitBreaker} accounting.
 *
 * <p>OpenSearch's breaker service is designed around per-request byte
 * accounting: callers call {@link CircuitBreaker#addEstimateBytesAndMaybeBreak}
 * before allocating and {@link CircuitBreaker#addWithoutBreaking} to release
 * the same bytes when done. Lance's inverted-index and metadata caches
 * do not fit that model: they are managed by the Rust {@code Session},
 * grow lazily on the first scan that touches them, and stay resident
 * until LRU evicts them. This helper reconciles the two models with a
 * polling loop that samples {@link org.lance.Session#sizeBytes()} on a
 * background scheduler and updates the breaker's accounting to match
 * the current cache footprint. The query paths (FTS scorer and knn
 * scorer) then call {@link #checkAndTrip(String)} before triggering a
 * native scan; if the breaker is already over its limit the call throws
 * {@link CircuitBreakingException} with the {@link CircuitBreaker.Durability#TRANSIENT}
 * flavour so OpenSearch surfaces a 429 to the client and the next
 * polling tick (or an idle-driven LRU eviction) can unblock the node.
 *
 * <p>The reference to the underlying {@link CircuitBreaker} is stored
 * as a static field because query builders receive a
 * {@code QueryShardContext} that does not expose the
 * {@code CircuitBreakerService}, and the query paths are the only place
 * where {@link #checkAndTrip(String)} has to run. In a single-node
 * production JVM this is exactly one instance; in multi-node
 * integration tests each node's {@code LancePlugin} instance writes its
 * own breaker here, so the last writer wins. That is acceptable
 * because the tests do not run concurrent shard workers that would
 * observe a stale reference during a lifecycle swap.
 */
public final class LanceCircuitBreaker {

    /** Name registered with {@code CircuitBreakerService}. Surfaced in {@code _nodes/stats/breaker}. */
    public static final String NAME = "lance_native";

    private static volatile CircuitBreaker BREAKER;
    private static final AtomicBoolean ENABLED = new AtomicBoolean(true);

    /**
     * Bytes last reported to the breaker. Used by the polling loop to
     * translate the {@code Session.sizeBytes()} absolute reading into
     * the delta {@link CircuitBreaker#addWithoutBreaking(long)} expects.
     */
    private static final AtomicLong LAST_REPORTED = new AtomicLong(0L);

    private LanceCircuitBreaker() {}

    /**
     * Install the breaker created by the plugin's
     * {@code CircuitBreakerPlugin.getCircuitBreaker(Settings)} return.
     * Called from {@code setCircuitBreaker} on the plugin.
     */
    public static void setBreaker(CircuitBreaker breaker) {
        BREAKER = breaker;
        // A newly installed breaker starts at 0 estimated bytes; reset
        // the last-reported counter so the first polling tick reports
        // an absolute reading rather than a delta against a stale
        // previous cycle.
        LAST_REPORTED.set(0L);
    }

    /** Toggle enforcement without swapping the breaker itself. Wired to the cluster-settings listener. */
    public static void setEnabled(boolean enabled) {
        ENABLED.set(enabled);
    }

    public static boolean isEnabled() {
        return ENABLED.get();
    }

    /** Expose the installed breaker for stats endpoints and tests. May be {@code null} before plugin start. */
    public static CircuitBreaker getBreaker() {
        return BREAKER;
    }

    /**
     * Reset for tests. Not intended for production callers; the plugin
     * lifecycle handles teardown by simply letting the JVM exit.
     */
    static void resetForTests() {
        BREAKER = null;
        ENABLED.set(true);
        LAST_REPORTED.set(0L);
    }

    /**
     * Update the breaker's accounting to reflect {@code currentBytes},
     * the freshly-sampled {@link org.lance.Session#sizeBytes()} reading.
     * Called by the plugin's polling scheduler. Never throws; the
     * breaker uses {@link CircuitBreaker#addWithoutBreaking(long)} so
     * that a real overshoot does not surface as an error from the poll
     * itself, only from the next {@link #checkAndTrip(String)} call.
     */
    public static void updateUsage(long currentBytes) {
        CircuitBreaker breaker = BREAKER;
        if (breaker == null) {
            return;
        }
        long previous = LAST_REPORTED.getAndSet(currentBytes);
        long delta = currentBytes - previous;
        if (delta != 0L) {
            breaker.addWithoutBreaking(delta);
        }
    }

    /**
     * Report the two native footprints the plugin owns as one reading:
     * the Lance Session's index and metadata caches and the fragment
     * path's off-heap column cache. Both share the
     * {@code lance.native_memory.limit} budget, so {@code _nodes/stats/breaker}
     * shows their sum under {@code lance_native}.
     */
    public static void updateUsage(long sessionBytes, long columnCacheBytes) {
        updateUsage(sessionBytes + columnCacheBytes);
    }

    /**
     * Throw {@link CircuitBreakingException} if the breaker is enabled
     * and its currently-tracked usage has caught up to (or exceeded)
     * the configured limit. Called before every native scan on the
     * shard-side query path.
     *
     * @param operation short label included in the exception message
     *                  so operators can see which query kind tripped
     *                  the breaker
     */
    public static void checkAndTrip(String operation) {
        CircuitBreaker breaker = BREAKER;
        if (breaker == null || !ENABLED.get()) {
            return;
        }
        long used = breaker.getUsed();
        long limit = breaker.getLimit();
        // Interpret non-positive limits (unset or explicitly zero) as
        // "no breaker", which matches how OpenSearch treats a noop
        // breaker: the check is a no-op rather than an always-trip.
        if (limit <= 0L) {
            return;
        }
        if (used >= limit) {
            String message = "Lance native memory limit reached during ["
                + operation
                + "]. Session and column cache used ["
                + used
                + "] bytes, limit ["
                + limit
                + "] bytes.";
            throw new CircuitBreakingException(message, used, limit, CircuitBreaker.Durability.TRANSIENT);
        }
    }
}
