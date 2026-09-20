/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.stats;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.lance.LanceCircuitBreaker;
import org.opensearch.lance.NativeMemoryLimit.IndexCacheSizing;
import org.opensearch.lance.engine.ColumnStore;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.query.LanceFtsQuery;

/**
 * Reads the plugin's node scoped caches into a {@link LanceNodeStats}.
 * Created by the plugin next to the {@link LanceWarmCache} and injected
 * into {@link TransportLanceStatsAction}; it holds no state of its own.
 *
 * <p>{@code native_memory.estimated_bytes} is what the {@code lance_native}
 * breaker currently accounts for, which the plugin's sampler last pushed
 * in ({@code lance.native_memory.circuit_breaker.poll_interval} ago at
 * most), while {@code session_bytes} and {@code column_store_bytes} are
 * read live, so the sum of the two can differ from the estimate by up to
 * one sampling interval of growth. The index cache capacity, shard count
 * and shard share are fixed at startup.
 */
public final class LanceStatsCollector {

    private final LanceWarmCache warmCache;
    private final LongSupplier sessionBytes;
    private final Supplier<IndexCacheSizing> indexCacheSizing;

    /**
     * @param warmCache        the node's snapshot cache, or {@code null}
     *                         when the plugin created none (every cache
     *                         figure is then zero)
     * @param sessionBytes     reads {@code Session.sizeBytes()} of the
     *                         shared Lance session, zero when none is
     *                         installed
     * @param indexCacheSizing reads the index cache sizing of the shared
     *                         Lance session, {@code null} when none is
     *                         installed (capacity, shards and share are
     *                         then zero)
     */
    public LanceStatsCollector(LanceWarmCache warmCache, LongSupplier sessionBytes, Supplier<IndexCacheSizing> indexCacheSizing) {
        this.warmCache = warmCache;
        this.sessionBytes = sessionBytes;
        this.indexCacheSizing = indexCacheSizing;
    }

    public LanceNodeStats collect() {
        CircuitBreaker breaker = LanceCircuitBreaker.getBreaker();
        long estimatedBytes = breaker == null ? 0L : breaker.getUsed();
        long session = sessionBytes.getAsLong();
        IndexCacheSizing sizing = indexCacheSizing.get();
        long indexCacheCapacity = sizing == null ? 0L : sizing.capacityBytes();
        int indexCacheShards = sizing == null ? 0 : sizing.shards();
        long indexCacheShardShare = sizing == null ? 0L : sizing.shardShareBytes();
        int probeLimit = LanceFtsQuery.subsetProbeLimit();
        if (warmCache == null) {
            return new LanceNodeStats(
                false,
                0,
                0,
                0L,
                0L,
                0L,
                0L,
                0L,
                0,
                0L,
                0L,
                0L,
                0L,
                estimatedBytes,
                session,
                indexCacheCapacity,
                indexCacheShards,
                indexCacheShardShare,
                probeLimit
            );
        }
        ColumnStore store = warmCache.columnStore();
        return new LanceNodeStats(
            warmCache.isEnabled(),
            warmCache.snapshotCount(),
            warmCache.retiredSnapshotCount(),
            warmCache.datasetOpenCount(),
            warmCache.snapshotBuildCount(),
            warmCache.snapshotHitCount(),
            store.allocatedBytes(),
            store.limitBytes(),
            store.entryCount(),
            store.hitCount(),
            store.loadCount(),
            store.evictionCount(),
            store.budgetMissCount(),
            estimatedBytes,
            session,
            indexCacheCapacity,
            indexCacheShards,
            indexCacheShardShare,
            probeLimit
        );
    }
}
