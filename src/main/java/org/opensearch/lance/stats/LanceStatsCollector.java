/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.stats;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.lance.LanceCircuitBreaker;
import org.opensearch.lance.NativeMemoryLimit.IndexCacheSizing;
import org.opensearch.lance.engine.ColumnStore;
import org.opensearch.lance.engine.HeapFallbackStats;
import org.opensearch.lance.engine.LanceIndexWarmer;
import org.opensearch.lance.engine.LanceLocalClones;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.plan.execute.FragmentPlanRefiner;
import org.opensearch.lance.query.ScanAdmission;
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
 * and shard share are fixed at startup. {@code column_store.heap_fallback_bytes}
 * and {@code heap_fallback_rejections} come from {@link HeapFallbackStats}
 * and are reported whether or not the node has a snapshot cache, since a
 * reader opened without one loads every column into heap. {@code warm_up}
 * is the {@link LanceIndexWarmer}'s view of every Lance-backed index the
 * node has seen, empty with the mode {@code none} when the node has no
 * warmer. {@code plan.refinements} and {@code plan.executed} read
 * {@link FragmentPlanRefiner}'s node wide counters: the pushed
 * operations the fragment executor moved to the Lucene side, per
 * reason, and the requests the Lance scan and Lucene each answered.
 */
public final class LanceStatsCollector {

    private final LanceWarmCache warmCache;
    private final LongSupplier sessionBytes;
    private final Supplier<IndexCacheSizing> indexCacheSizing;
    private final LanceIndexWarmer indexWarmer;
    private final Supplier<Map<String, LanceLocalClones.CloneStat>> localClones;

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
        this(warmCache, sessionBytes, indexCacheSizing, null, null);
    }

    /**
     * @param indexWarmer the node's index warmer, or {@code null} when the
     *                    plugin created none (the warm-up block is then
     *                    empty)
     */
    public LanceStatsCollector(
        LanceWarmCache warmCache,
        LongSupplier sessionBytes,
        Supplier<IndexCacheSizing> indexCacheSizing,
        LanceIndexWarmer indexWarmer
    ) {
        this(warmCache, sessionBytes, indexCacheSizing, indexWarmer, null);
    }

    /**
     * @param localClones reads this node's {@code node_local} clone
     *                    directories (bytes and recorded source version
     *                    per index), or {@code null} when the plugin
     *                    created no clone service (the {@code local_clones}
     *                    block is then empty)
     */
    public LanceStatsCollector(
        LanceWarmCache warmCache,
        LongSupplier sessionBytes,
        Supplier<IndexCacheSizing> indexCacheSizing,
        LanceIndexWarmer indexWarmer,
        Supplier<Map<String, LanceLocalClones.CloneStat>> localClones
    ) {
        this.warmCache = warmCache;
        this.sessionBytes = sessionBytes;
        this.indexCacheSizing = indexCacheSizing;
        this.indexWarmer = indexWarmer;
        this.localClones = localClones;
    }

    /** The node's cache figures with no index block; {@link #collect(List)} adds the shard readers. */
    public LanceNodeStats collect() {
        return collect(List.of());
    }

    /**
     * @param indices the shard readers of the Lance-backed indexes this
     *                node hosts, read by the transport action from the
     *                node's index services
     */
    public LanceNodeStats collect(List<LanceNodeStats.IndexReaderStats> indices) {
        CircuitBreaker breaker = LanceCircuitBreaker.getBreaker();
        long estimatedBytes = breaker == null ? 0L : breaker.getUsed();
        long session = sessionBytes.getAsLong();
        IndexCacheSizing sizing = indexCacheSizing.get();
        long indexCacheCapacity = sizing == null ? 0L : sizing.capacityBytes();
        int indexCacheShards = sizing == null ? 0 : sizing.shards();
        long indexCacheShardShare = sizing == null ? 0L : sizing.shardShareBytes();
        int probeLimit = LanceFtsQuery.subsetProbeLimit();
        Map<String, Long> admissionRejections = ScanAdmission.rejectionsByKind();
        long admissionLastEstimate = ScanAdmission.lastEstimateBytes();
        String admissionLastKind = ScanAdmission.lastKind();
        // One reading serves both figures, so the reported credit is the
        // one a decision made at the reported available memory would use.
        long admissionAvailableReading = ScanAdmission.availablePhysicalMemoryBytes();
        long admissionAvailable = Math.max(0L, admissionAvailableReading);
        long admissionRetained = ScanAdmission.retainedCreditBytes(admissionAvailableReading);
        long heapFallbackBytes = HeapFallbackStats.bytes();
        long heapFallbackRejections = HeapFallbackStats.rejections();
        String warmUpMode = indexWarmer == null ? "none" : indexWarmer.mode().settingValue();
        List<LanceWarmUpStatus> warmUps = new ArrayList<>();
        if (indexWarmer != null) {
            for (LanceIndexWarmer.TableStatus status : indexWarmer.statuses()) {
                warmUps.add(LanceWarmUpStatus.of(status));
            }
            warmUps.sort((a, b) -> a.index().compareTo(b.index()));
        }
        List<LanceNodeStats.LocalCloneStats> cloneStats = new ArrayList<>();
        if (localClones != null) {
            for (Map.Entry<String, LanceLocalClones.CloneStat> entry : localClones.get().entrySet()) {
                cloneStats.add(
                    new LanceNodeStats.LocalCloneStats(entry.getKey(), entry.getValue().bytes(), entry.getValue().sourceVersion())
                );
            }
            cloneStats.sort((a, b) -> a.index().compareTo(b.index()));
        }
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
                heapFallbackBytes,
                heapFallbackRejections,
                estimatedBytes,
                session,
                indexCacheCapacity,
                indexCacheShards,
                indexCacheShardShare,
                probeLimit,
                admissionRejections,
                admissionLastEstimate,
                admissionLastKind,
                admissionAvailable,
                admissionRetained,
                warmUpMode,
                warmUps,
                indices,
                cloneStats,
                0,
                0L,
                FragmentPlanRefiner.refinementCounts(),
                FragmentPlanRefiner.executedCounts()
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
            heapFallbackBytes,
            heapFallbackRejections,
            estimatedBytes,
            session,
            indexCacheCapacity,
            indexCacheShards,
            indexCacheShardShare,
            probeLimit,
            admissionRejections,
            admissionLastEstimate,
            admissionLastKind,
            admissionAvailable,
            admissionRetained,
            warmUpMode,
            warmUps,
            indices,
            cloneStats,
            warmCache.tableStatistics().size(),
            warmCache.tableStatistics().collectMillisTotal(),
            FragmentPlanRefiner.refinementCounts(),
            FragmentPlanRefiner.executedCounts()
        );
    }
}
