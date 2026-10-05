/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Session;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.indices.breaker.BreakerSettings;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;

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
     * {@code plugins.lance.native_memory.limit} budget, so {@code _nodes/stats/breaker}
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

    /**
     * The breaker the plugin registers through
     * {@code CircuitBreakerPlugin.getCircuitBreaker}: named {@link #NAME},
     * limited to {@link LanceSettings#NATIVE_MEMORY_LIMIT_SETTING} so an
     * operator configures one number, with an overhead of 1.0 because the
     * sampler pushes measured usage rather than an estimate, and
     * {@link CircuitBreaker.Durability#TRANSIENT} because an LRU eviction
     * or the next sample is expected to clear the condition without an
     * operator, which OpenSearch surfaces as a 429.
     */
    public static BreakerSettings breakerSettings(Settings settings) {
        String rawLimit = LanceSettings.NATIVE_MEMORY_LIMIT_SETTING.get(settings);
        long limitBytes = NativeMemoryLimit.parse(rawLimit, LanceSettings.NATIVE_MEMORY_LIMIT_SETTING.getKey());
        return new BreakerSettings(NAME, limitBytes, 1.0, CircuitBreaker.Type.MEMORY, CircuitBreaker.Durability.TRANSIENT);
    }

    /**
     * The loop that keeps the breaker's accounting aligned with the
     * native footprint: at {@link LanceSettings#NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING}
     * it reads {@code Session.sizeBytes()} and the column cache's bytes
     * and hands their sum to {@link #updateUsage(long, long)}. It runs on
     * the generic pool so it takes no capacity from the search or write
     * executors. {@link LanceSettings#NATIVE_MEMORY_CB_ENABLED_SETTING}
     * and the interval are dynamic; the sampler registers for both and
     * reschedules itself when the interval changes.
     */
    public static final class Sampler implements Closeable {

        private static final Logger LOGGER = LogManager.getLogger(Sampler.class);

        private final ThreadPool threadPool;
        private final LongSupplier columnCacheBytes;
        private volatile Cancellable task;
        private volatile TimeValue interval;

        private Sampler(ThreadPool threadPool, LongSupplier columnCacheBytes, TimeValue interval) {
            this.threadPool = threadPool;
            this.columnCacheBytes = columnCacheBytes;
            this.interval = interval;
            this.task = schedule(interval);
        }

        /**
         * Apply the enabled flag, start sampling at the configured
         * interval and register for changes of both.
         *
         * @param columnCacheBytes the column cache's current bytes, read
         *        on every sample
         */
        public static Sampler start(
            ThreadPool threadPool,
            Settings settings,
            ClusterSettings clusterSettings,
            LongSupplier columnCacheBytes
        ) {
            setEnabled(LanceSettings.NATIVE_MEMORY_CB_ENABLED_SETTING.get(settings));
            clusterSettings.addSettingsUpdateConsumer(LanceSettings.NATIVE_MEMORY_CB_ENABLED_SETTING, LanceCircuitBreaker::setEnabled);
            Sampler sampler = new Sampler(threadPool, columnCacheBytes, LanceSettings.NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING.get(settings));
            clusterSettings.addSettingsUpdateConsumer(LanceSettings.NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING, sampler::setInterval);
            return sampler;
        }

        private Cancellable schedule(TimeValue every) {
            Runnable sample = () -> {
                try {
                    Session session = LanceRegistry.currentSession();
                    if (session == null || session.isClosed()) {
                        return;
                    }
                    updateUsage(session.sizeBytes(), columnCacheBytes.getAsLong());
                } catch (Throwable t) {
                    // A failed reading leaves the accounting as it was for
                    // one more cycle; the scheduler must not see the throw.
                    LOGGER.warn("lance_native circuit breaker poll iteration failed", t);
                }
            };
            return threadPool.scheduleWithFixedDelay(sample, every, ThreadPool.Names.GENERIC);
        }

        /** Reschedule at {@code newInterval}; the same interval leaves the current schedule in place. */
        synchronized void setInterval(TimeValue newInterval) {
            if (newInterval.equals(interval)) {
                return;
            }
            Cancellable current = task;
            if (current != null) {
                current.cancel();
            }
            interval = newInterval;
            task = schedule(newInterval);
            LOGGER.info("lance_native circuit breaker poll interval updated to [{}]", newInterval);
        }

        @Override
        public void close() {
            Cancellable current = task;
            if (current != null) {
                current.cancel();
                task = null;
            }
        }
    }
}
