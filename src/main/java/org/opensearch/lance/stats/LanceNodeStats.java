/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.stats;

import java.io.IOException;
import java.util.Objects;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * One node's view of the plugin's caches at the moment
 * {@link LanceStatsCollector#collect()} ran: the snapshot cache, the
 * off-heap column store and the heap columns its misses put on the
 * request breaker, the native memory the {@code lance_native}
 * breaker accounts for, the index cache's capacity and shard layout and
 * the full-text probe limit in force. Read only; every number is a plain
 * counter or gauge read from the owning component.
 *
 * <p>Rendered as the {@code snapshots}, {@code column_store},
 * {@code native_memory} and {@code fts} objects of one node in
 * {@code GET /_lance/stats}.
 */
public final class LanceNodeStats implements Writeable, ToXContentFragment {

    private final boolean cacheEnabled;
    private final int snapshotCount;
    private final int retiredSnapshotCount;
    private final long datasetOpenCount;
    private final long snapshotBuildCount;
    private final long snapshotHitCount;

    private final long columnStoreBytes;
    private final long columnStoreLimitBytes;
    private final int columnStoreEntries;
    private final long columnStoreHits;
    private final long columnStoreLoads;
    private final long columnStoreEvictions;
    private final long columnStoreBudgetMisses;
    private final long heapFallbackBytes;
    private final long heapFallbackRejections;

    private final long nativeEstimatedBytes;
    private final long sessionBytes;
    private final long indexCacheCapacityBytes;
    private final int indexCacheShards;
    private final long indexCacheShardShareBytes;

    private final int ftsSubsetProbeLimit;

    public LanceNodeStats(
        boolean cacheEnabled,
        int snapshotCount,
        int retiredSnapshotCount,
        long datasetOpenCount,
        long snapshotBuildCount,
        long snapshotHitCount,
        long columnStoreBytes,
        long columnStoreLimitBytes,
        int columnStoreEntries,
        long columnStoreHits,
        long columnStoreLoads,
        long columnStoreEvictions,
        long columnStoreBudgetMisses,
        long heapFallbackBytes,
        long heapFallbackRejections,
        long nativeEstimatedBytes,
        long sessionBytes,
        long indexCacheCapacityBytes,
        int indexCacheShards,
        long indexCacheShardShareBytes,
        int ftsSubsetProbeLimit
    ) {
        this.cacheEnabled = cacheEnabled;
        this.snapshotCount = snapshotCount;
        this.retiredSnapshotCount = retiredSnapshotCount;
        this.datasetOpenCount = datasetOpenCount;
        this.snapshotBuildCount = snapshotBuildCount;
        this.snapshotHitCount = snapshotHitCount;
        this.columnStoreBytes = columnStoreBytes;
        this.columnStoreLimitBytes = columnStoreLimitBytes;
        this.columnStoreEntries = columnStoreEntries;
        this.columnStoreHits = columnStoreHits;
        this.columnStoreLoads = columnStoreLoads;
        this.columnStoreEvictions = columnStoreEvictions;
        this.columnStoreBudgetMisses = columnStoreBudgetMisses;
        this.heapFallbackBytes = heapFallbackBytes;
        this.heapFallbackRejections = heapFallbackRejections;
        this.nativeEstimatedBytes = nativeEstimatedBytes;
        this.sessionBytes = sessionBytes;
        this.indexCacheCapacityBytes = indexCacheCapacityBytes;
        this.indexCacheShards = indexCacheShards;
        this.indexCacheShardShareBytes = indexCacheShardShareBytes;
        this.ftsSubsetProbeLimit = ftsSubsetProbeLimit;
    }

    public LanceNodeStats(StreamInput in) throws IOException {
        this.cacheEnabled = in.readBoolean();
        this.snapshotCount = in.readVInt();
        this.retiredSnapshotCount = in.readVInt();
        this.datasetOpenCount = in.readVLong();
        this.snapshotBuildCount = in.readVLong();
        this.snapshotHitCount = in.readVLong();
        this.columnStoreBytes = in.readVLong();
        this.columnStoreLimitBytes = in.readVLong();
        this.columnStoreEntries = in.readVInt();
        this.columnStoreHits = in.readVLong();
        this.columnStoreLoads = in.readVLong();
        this.columnStoreEvictions = in.readVLong();
        this.columnStoreBudgetMisses = in.readVLong();
        this.heapFallbackBytes = in.readVLong();
        this.heapFallbackRejections = in.readVLong();
        this.nativeEstimatedBytes = in.readLong();
        this.sessionBytes = in.readLong();
        this.indexCacheCapacityBytes = in.readVLong();
        this.indexCacheShards = in.readVInt();
        this.indexCacheShardShareBytes = in.readVLong();
        this.ftsSubsetProbeLimit = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(cacheEnabled);
        out.writeVInt(snapshotCount);
        out.writeVInt(retiredSnapshotCount);
        out.writeVLong(datasetOpenCount);
        out.writeVLong(snapshotBuildCount);
        out.writeVLong(snapshotHitCount);
        out.writeVLong(columnStoreBytes);
        out.writeVLong(columnStoreLimitBytes);
        out.writeVInt(columnStoreEntries);
        out.writeVLong(columnStoreHits);
        out.writeVLong(columnStoreLoads);
        out.writeVLong(columnStoreEvictions);
        out.writeVLong(columnStoreBudgetMisses);
        out.writeVLong(heapFallbackBytes);
        out.writeVLong(heapFallbackRejections);
        out.writeLong(nativeEstimatedBytes);
        out.writeLong(sessionBytes);
        out.writeVLong(indexCacheCapacityBytes);
        out.writeVInt(indexCacheShards);
        out.writeVLong(indexCacheShardShareBytes);
        out.writeVInt(ftsSubsetProbeLimit);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("snapshots");
        builder.field("enabled", cacheEnabled);
        builder.field("count", snapshotCount);
        builder.field("retired", retiredSnapshotCount);
        builder.field("dataset_open_count", datasetOpenCount);
        builder.field("snapshot_build_count", snapshotBuildCount);
        builder.field("snapshot_hit_count", snapshotHitCount);
        builder.endObject();

        builder.startObject("column_store");
        builder.field("bytes", columnStoreBytes);
        builder.field("limit_bytes", columnStoreLimitBytes);
        builder.field("entries", columnStoreEntries);
        builder.field("hits", columnStoreHits);
        builder.field("loads", columnStoreLoads);
        builder.field("evictions", columnStoreEvictions);
        builder.field("budget_misses", columnStoreBudgetMisses);
        builder.field("heap_fallback_bytes", heapFallbackBytes);
        builder.field("heap_fallback_rejections", heapFallbackRejections);
        builder.endObject();

        builder.startObject("native_memory");
        builder.field("estimated_bytes", nativeEstimatedBytes);
        builder.field("session_bytes", sessionBytes);
        builder.field("column_store_bytes", columnStoreBytes);
        builder.field("index_cache_capacity", indexCacheCapacityBytes);
        builder.field("index_cache_shards", indexCacheShards);
        builder.field("index_cache_shard_share", indexCacheShardShareBytes);
        builder.endObject();

        builder.startObject("fts");
        builder.field("subset_probe_limit", ftsSubsetProbeLimit);
        builder.endObject();
        return builder;
    }

    public boolean cacheEnabled() {
        return cacheEnabled;
    }

    public int snapshotCount() {
        return snapshotCount;
    }

    public int retiredSnapshotCount() {
        return retiredSnapshotCount;
    }

    public long datasetOpenCount() {
        return datasetOpenCount;
    }

    public long snapshotBuildCount() {
        return snapshotBuildCount;
    }

    public long snapshotHitCount() {
        return snapshotHitCount;
    }

    public long columnStoreBytes() {
        return columnStoreBytes;
    }

    public long columnStoreLimitBytes() {
        return columnStoreLimitBytes;
    }

    public int columnStoreEntries() {
        return columnStoreEntries;
    }

    public long columnStoreHits() {
        return columnStoreHits;
    }

    public long columnStoreLoads() {
        return columnStoreLoads;
    }

    public long columnStoreEvictions() {
        return columnStoreEvictions;
    }

    public long columnStoreBudgetMisses() {
        return columnStoreBudgetMisses;
    }

    /** Heap bytes open readers on the node currently have charged to the request breaker for columns the store could not hold. */
    public long heapFallbackBytes() {
        return heapFallbackBytes;
    }

    /** Heap column loads the request breaker refused (each one ended a request with HTTP 429). */
    public long heapFallbackRejections() {
        return heapFallbackRejections;
    }

    public long nativeEstimatedBytes() {
        return nativeEstimatedBytes;
    }

    public long sessionBytes() {
        return sessionBytes;
    }

    public long indexCacheCapacityBytes() {
        return indexCacheCapacityBytes;
    }

    public int indexCacheShards() {
        return indexCacheShards;
    }

    public long indexCacheShardShareBytes() {
        return indexCacheShardShareBytes;
    }

    public int ftsSubsetProbeLimit() {
        return ftsSubsetProbeLimit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LanceNodeStats other)) {
            return false;
        }
        return cacheEnabled == other.cacheEnabled
            && snapshotCount == other.snapshotCount
            && retiredSnapshotCount == other.retiredSnapshotCount
            && datasetOpenCount == other.datasetOpenCount
            && snapshotBuildCount == other.snapshotBuildCount
            && snapshotHitCount == other.snapshotHitCount
            && columnStoreBytes == other.columnStoreBytes
            && columnStoreLimitBytes == other.columnStoreLimitBytes
            && columnStoreEntries == other.columnStoreEntries
            && columnStoreHits == other.columnStoreHits
            && columnStoreLoads == other.columnStoreLoads
            && columnStoreEvictions == other.columnStoreEvictions
            && columnStoreBudgetMisses == other.columnStoreBudgetMisses
            && heapFallbackBytes == other.heapFallbackBytes
            && heapFallbackRejections == other.heapFallbackRejections
            && nativeEstimatedBytes == other.nativeEstimatedBytes
            && sessionBytes == other.sessionBytes
            && indexCacheCapacityBytes == other.indexCacheCapacityBytes
            && indexCacheShards == other.indexCacheShards
            && indexCacheShardShareBytes == other.indexCacheShardShareBytes
            && ftsSubsetProbeLimit == other.ftsSubsetProbeLimit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            cacheEnabled,
            snapshotCount,
            retiredSnapshotCount,
            datasetOpenCount,
            snapshotBuildCount,
            snapshotHitCount,
            columnStoreBytes,
            columnStoreLimitBytes,
            columnStoreEntries,
            columnStoreHits,
            columnStoreLoads,
            columnStoreEvictions,
            columnStoreBudgetMisses,
            heapFallbackBytes,
            heapFallbackRejections,
            nativeEstimatedBytes,
            sessionBytes,
            indexCacheCapacityBytes,
            indexCacheShards,
            indexCacheShardShareBytes,
            ftsSubsetProbeLimit
        );
    }
}
